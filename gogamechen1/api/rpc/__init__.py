# -*- coding:utf-8 -*-
import os
import sys
import time
import signal
import shutil
import json
import re
import contextlib
import eventlet
import psutil

from eventlet.semaphore import Semaphore

from simpleutil.utils import argutils
from simpleutil.utils import singleton
from simpleutil.log import log as logging
from simpleutil.config import cfg

from simpleutil.utils import zlibutils
from simpleutil.utils import systemutils

from simpleservice.loopingcall import IntervalLoopinTask

from goperation import threadpool
from goperation.utils import safe_fork
from goperation.utils import umask
from goperation.filemanager.exceptions import NoFileFound
from goperation.manager.api import get_http
from goperation.manager import common as manager_common
from goperation.manager.rpc.agent.application.base import AppEndpointBase

from goperation.manager.utils import resultutils
from goperation.manager.rpc.exceptions import RpcCtxtException
from goperation.manager.rpc.exceptions import RpcEntityError
from goperation.manager.rpc.exceptions import RpcTargetLockException


from gogamechen1 import common
from gogamechen1 import utils

from gogamechen1.api import gconfig
from gogamechen1.api import gfile
from gogamechen1.api.exceptions import MergeException
from gogamechen1.api.client import GogameChen1DBClient
from gogamechen1.api.rpc.config import gameserver_group
from gogamechen1.api.rpc.config import crossserver_group
from gogamechen1.api.rpc.config import gmserver_group
from gogamechen1.api.rpc.config import register_opts
from gogamechen1.api.rpc.config import agent_opts

from gogamechen1.api.rpc.taskflow import create as taskcreate
from gogamechen1.api.rpc.taskflow import upgrade as taskupgrade
from gogamechen1.api.rpc.taskflow import hotfix as taskhotfix
from gogamechen1.api.rpc.taskflow import merge as taskmerge

if systemutils.POSIX:
    from simpleutil.utils.systemutils import posix

from simpleutil.utils.systemutils import unlimit_core
from simpleutil.utils.systemutils import open_file_limit

CONF = cfg.CONF
CONF.register_group(gameserver_group)
CONF.register_group(crossserver_group)
CONF.register_group(gmserver_group)
register_opts()


LOG = logging.getLogger(__name__)

COREPATEN = re.compile('^core\.[0-9]+?$', re.IGNORECASE)

def count_timeout(ctxt, kwargs):
    finishtime = ctxt.get('finishtime')
    timeout = kwargs.pop('timeout', None) if kwargs else None
    if finishtime is None:
        return timeout if timeout is not None else 15
    _timeout = finishtime - int(time.time())
    if timeout is None:
        return _timeout
    return min(_timeout, timeout)


def AsyncActionResult(action, stores):
    def result(_entity, code, msg=None):
        entity = stores[_entity]
        if not entity.get('areas'):
            areas = 'N/A'
        else:
            areas = ['%d:%s' % (area.get('show_id'), area.get('areaname'))
                     for area in entity.get('areas')]
            areas = ','.join(areas)
        if not entity.get('pid'):
            pid = 'N/A'
        else:
            pid = str(entity.get('pid'))
        msg = msg or '%s entity %d %s' % (action, _entity,
                                          'success' if code == manager_common.RESULT_SUCCESS
                                          else 'fail')

        return dict(detail_id=_entity,
                    resultcode=code,
                    result='%s|%s|%s' % (areas, pid, msg))

    return result


class CreateResult(resultutils.AgentRpcResult):
    def __init__(self, agent_id, ctxt,
                 resultcode, result,
                 connection, ports, databases):
        super(CreateResult, self).__init__(agent_id, ctxt, resultcode, result)
        self.connection = connection
        self.ports = ports
        self.databases = databases

    def to_dict(self):
        ret_dict = super(CreateResult, self).to_dict()
        ret_dict.setdefault('databases', self.databases)
        ret_dict.setdefault('ports', self.ports)
        ret_dict.setdefault('connection', self.connection)
        return ret_dict


class EntityProcessCheckTasker(IntervalLoopinTask):
    """
    周期性entity进程检查
    """

    def __init__(self, endpoint):
        self.endpoint = endpoint
        # 有死过进程的entity列表
        self.deads = dict()
        conf = CONF[common.NAME]
        self.pop_from_deads = conf.pop_from_deads
        self.auto_restart_times = conf.auto_restart_times
        super(EntityProcessCheckTasker, self).__init__(periodic_interval=conf.periodic_interval * 60,
                                                       initial_delay=self.pop_from_deads,
                                                       stop_on_exception=False)

    def __call__(self, *args, **kwargs):
        now = int(time.time())
        entitys = []
        for entity in self.endpoint.konwn_appentitys:
            if self.endpoint.konwn_appentitys[entity]['started']:
                entitys.append(entity)
            else:
                self.deads.pop(entity, None)
        if entitys:
            proc_snapshot = utils.find_process()
            for entity in entitys:
                if not self.endpoint.konwn_appentitys[entity]['started']:
                    self.deads.pop(entity, None)
                    continue
                status = self.endpoint.konwn_appentitys[entity].get('status')
                if status != common.OK:
                    with self.endpoint.lock(entity):
                        self.endpoint.konwn_appentitys[entity]['started'] = False
                    self.deads.pop(entity, None)
                    continue
                if self.endpoint._entity_process(entity, proc_snapshot):
                    info = self.deads.pop(entity, None)
                    if info and (now - info.get('time') < self.pop_from_deads):
                        self.deads.setdefault(entity, info)
                    continue
                info = self.deads.pop(entity, None)
                if not info:
                    info = dict(time=now, times=1)
                else:
                    info['times'] += 1
                    info['time'] = now
                if info.get('times') > self.auto_restart_times:
                    LOG.warning('Entity process dead over auto restart max times')
                    self.endpoint.notify(entity, 'dead')
                    with self.endpoint.lock(entity):
                        self.endpoint.konwn_appentitys[entity]['started'] = False
                    continue
                self.deads.setdefault(entity, info)
                LOG.info('Try restart entity process %d times when it is dead' % info.get('times'))
                eventlet.spawn_n(self.endpoint.start_entity, entity)
                # eventlet.sleep(0)
        else:
            for entity in self.deads.keys():
                info = self.deads[entity]
                # 存活时间足够长,从死亡列表中移除
                if now - info.get('time') >= self.pop_from_deads:
                    self.deads.pop(entity, None)



@singleton.singleton
class Application(AppEndpointBase):

    def __init__(self, manager):
        group = CONF.find_group(common.NAME)
        CONF.register_opts(agent_opts, group)
        super(Application, self).__init__(manager, group.name)
        self.client = GogameChen1DBClient(get_http())
        self.delete_tokens = {}
        self.konwn_appentitys = {}
        # Merger lock
        self.mlock = Semaphore(1)

    @property
    def apppathname(self):
        return 'gogame'

    def entity_user(self, entity):
        return 'gogamechen1-%d' % entity

    def entity_group(self, entity):
        return 'gogamechen1'

    def logbakup(self, entity):
        return os.path.join(self.bakpath(entity), 'logbak-%d' % entity)

    def clean_expired(self):
        """
        重写清理函数
        同时清理endpoint备份目录和entity备份目录
        """
        super(Application, self).clean_expired()
        eventlet.sleep(0)
        for entity in self.entitys:
            backup = self.bakpath(entity)
            self.clean(backup, 864000)
            eventlet.sleep(0)

    def pre_start(self, external_objects):
        super(AppEndpointBase, self).pre_start(external_objects)
        conf = CONF[common.NAME]
        external_objects.update({'gogamechen1-aff': conf.agent_affinity})
        if conf.auto_restart_times:
            self.manager.periodic_tasks.append(EntityProcessCheckTasker(self))

    def post_start(self):
        super(Application, self).post_start()
        pids = utils.find_process()
        # reflect entity objtype
        if self.entitys:
            LOG.info('Try reflect entity objtype and group info')
            entitymaps = self.client.appentitys(entitys=self.entitys)['data']
            if len(entitymaps) != len(self.entitys):
                raise RuntimeError('Gogamechen1 entity count error, miss some entity')
            for entityinfo in entitymaps:
                _entity = entityinfo.get('entity')
                objtype = entityinfo.get('objtype')
                group_id = entityinfo.get('group_id')
                areas = entityinfo.get('areas')
                status = entityinfo.get('status')
                opentime= entityinfo.get('opentime')
                if _entity in self.konwn_appentitys:
                    raise RuntimeError('App Entity %d Duplicate' % _entity)
                LOG.info('Entity %d type %s, group %d' % (_entity, objtype, group_id))
                self.konwn_appentitys.setdefault(_entity, dict(objtype=objtype,
                                                               # group_id=group_id,
                                                               status=status,
                                                               areas=areas,
                                                               opentime=opentime,
                                                               started=False,
                                                               pid=None))
        for entity in self.entitys:
            # INIT backup path
            if os.path.exists(self.apppath(entity)):
                bakpath = self.bakpath(entity)
                if not os.path.exists(bakpath):
                    try:
                        os.makedirs(bakpath, mode=0o755)
                    except (OSError, IOError):
                        LOG.error('Make path for backup entity log fail')
                    finally:
                        systemutils.chown(bakpath, self.entity_user(entity), self.entity_group(entity))
            _pid = self._find_from_pids(entity, self.konwn_appentitys[entity].get('objtype'), pids)
            # find entity pid
            if _pid:
                LOG.info('App entity %d is running at %d' % (entity, _pid))
                self.konwn_appentitys[entity]['pid'] = _pid
                self.konwn_appentitys[entity]['started'] = True

    def _esure(self, entity, objtype, proc):
        datadir = False
        runuser = False
        _execfile = os.path.join(self.apppath(entity), 'bin', objtype)
        if proc.get('exe') != _execfile:
            return False
        if proc.get('username') == self.entity_user(entity):
            runuser = True
        if proc.get('pwd') == self.apppath(entity):
            datadir = True
        if datadir and runuser:
            return True
        if datadir and not runuser:
            LOG.error('entity %d with %s run user error' % (entity, self.apppath(entity)))
            raise ValueError('Runuser not %s' % self.entity_user(entity))
        return False

    def _find_from_pids(self, entity, objtype, pids=None):
        if not pids:
            pids = utils.find_process(objtype)
        for proc in pids:
            if self._esure(entity, objtype, proc):
                return proc.get('pid')

    def _objtype(self, entity):
        return self.konwn_appentitys[entity].get('objtype')

    def _objconf(self, entity, objtype=None):
        if not objtype:
            objtype = self._objtype(entity)
        return os.path.join(self.apppath(entity), 'conf', '%s.json' % objtype)

    def _entity_version(self, entity):
        vfile = os.path.join(self.apppath(entity), 'config', 'version.json')
        if not os.path.exists(vfile) or not os.path.isfile(vfile):
            return 'unkonwn'
        if os.path.getsize(vfile) > 1000:
            return 'oversize'
        with open(vfile, 'r') as f:
            lines = f.readlines()
            try:
                version = '%s %s %s' % (lines[7].strip(), lines[-3].strip(), lines[-2].strip())
            except IndexError:
                return 'unkonwn'
            else:
                return version

    def local_database_info(self, entity, subtype):
        return gconfig.deacidizing(self._objconf(entity), common.DATADB)

    @contextlib.contextmanager
    def _allocate_port(self, entity, objtype, ports):
        need = common.POSTS_COUNT[objtype]

        if ports is None:
            ports = [None] * need

        if len(ports) != need:
            raise ValueError('%s need %d ports, but found %d' % (objtype,
                                                                 need,
                                                                 len(ports)))
        with self.manager.frozen_ports(common.NAME, entity, ports=ports) as ports:
            ports = sorted(ports)
            yield ports

    def _free_ports(self, entity):
        ports = self.manager.allocked_ports.get(common.NAME)[entity]
        self.manager.free_ports(ports)

    def _get_ports(self, entity):
        return sorted([port for port in self.entitys_map[entity]])

    def _entity_process(self, entity, pids=None):
        entityinfo = self.konwn_appentitys.get(entity)
        if not entityinfo:
            return ValueError('Entity info not found')
        objtype = entityinfo.get('objtype')
        _pid = entityinfo.get('pid')
        if _pid:
            try:
                p = psutil.Process(pid=_pid)
                info = dict(pid=p.pid, name=p.name(), exe=p.exe(), pwd=p.cwd(), username=p.username())
                # pid正确
                if self._esure(entity, objtype, info):
                    setattr(p, 'info', info)
                    return p
                # pid错误
                else:
                    _pid = None
            except psutil.NoSuchProcess:
                _pid = None
        if not _pid:
            # 重新找pid
            _pid = self._find_from_pids(entity, objtype, pids=pids)
        # 还是没有对应pid
        if not _pid:
            self.konwn_appentitys[entity]['pid'] = None
            # 程序不存在
            return None
        try:
            # 再次验证pid
            p = psutil.Process(pid=_pid)
            info = dict(pid=p.pid, name=p.name(), exe=p.exe(), pwd=p.cwd(), username=p.username())
            if self._esure(entity, objtype, info):
                setattr(p, 'info', info)
                self.konwn_appentitys[entity]['pid'] = _pid
                return p
            else:
                # 还是验证错误
                self.konwn_appentitys[entity]['pid'] = None
                return None
        except psutil.NoSuchProcess:
            self.konwn_appentitys[entity]['pid'] = None
            return None

    def flush_config(self, entity, databases=None,
                     opentime=None, chiefs=None):
        eventlet.sleep(0.01)
        objtype = self._objtype(entity)
        ports = self._get_ports(entity)

        miss = common.POSTS_COUNT[objtype] - len(ports)
        if miss:
            if miss < 0:
                LOG.error('Miss ports count less then 0')
                raise ValueError('need %d ports, but %d found' % (common.POSTS_COUNT[objtype],
                                                                  len(ports)))
            LOG.info('%s.%d port miss some port' % (objtype, entity))
            with self.manager.frozen_ports(common.NAME, entity, ports=[None] * miss) as more_posts:
                self.client.ports_add(agent_id=self.manager.agent_id, endpoint=common.NAME,
                                      entity=entity, ports=more_posts)
                LOG.info('Miss port of %s.%d, success allocate' % (objtype, entity))
            ports = self._get_ports(entity)
        try:
            areas = self.konwn_appentitys[entity].get('areas')
            cfile = self._objconf(entity, objtype)
            databases = gconfig.format_databases(objtype, cfile, databases)
            chiefs = gconfig.format_chiefs(objtype, cfile, chiefs)
            flag = gconfig.server_flag(objtype, cfile)
            opentime = self.konwn_appentitys[entity].get('opentime') if not opentime else opentime
            opentime = gconfig.format_opentime(objtype, cfile, opentime)
            confobj = gconfig.make(objtype, self.logpath(entity),
                                   self.manager.local_ip, ports,
                                   entity, areas,
                                   databases, opentime, chiefs, flag)
        except Exception:
            LOG.exception('flush config fail')
            raise
        with open(cfile, 'wb') as f:
            json.dump(confobj, f, indent=4, ensure_ascii=False)
            f.write('\n')
        LOG.info('Make config for %s.%d success' % (objtype, entity))
        systemutils.chown(cfile, self.entity_user(entity), self.entity_group(entity))

    def _create_entity(self, entity, objtype, appfile, databases, timeout, ports=None):
        self.filemanager.find(appfile)
        with self.lock(entity):
            if entity in self.entitys:
                raise RpcEntityError(endpoint=common.NAME, entity=entity, reason='Entity duplicate')
            with self._prepare_entity_path(entity):
                bakpath = self.bakpath(entity)
                os.makedirs(bakpath, mode=0o755)
                systemutils.chown(bakpath, self.entity_user(entity), self.entity_group(entity))
                confdir = os.path.split(self._objconf(entity, objtype))[0]
                os.makedirs(confdir, mode=0o755)
                systemutils.chown(confdir, self.entity_user(entity), self.entity_group(entity))
                with self._allocate_port(entity, objtype, ports):
                    try:
                        middleware = taskcreate.create_entity(self, entity, objtype, databases,
                                                              appfile, timeout)
                    except Exception:
                        LOG.exception('prepare create taskflow error')
                        raise
                    # 有步骤失败
                    if not middleware.success:
                        if middleware.waiter is not None:
                            try:
                                middleware.waiter.stop()
                                middleware.waiter.wait()
                            except Exception as e:
                                LOG.error('Stop waiter catch error %s' % e.__class__.__name__)
                            finally:
                                middleware.waiter = None
                        LOG.error('create middleware result %s' % str(middleware))
                        raise RpcEntityError(endpoint=common.NAME, entity=entity,
                                             reason=str(middleware))

                    def _extract_wait():
                        if middleware.waiter is not None:
                            try:
                                middleware.waiter.wait()
                            except Exception:
                                LOG.error('Wait extract entity file catch error')
                                if LOG.isEnabledFor(logging.DEBUG):
                                    LOG.exception('extract fail')
                            finally:
                                middleware.waiter = None
                        self.manager.change_performance()

                    # 等待解压完成
                    threadpool.add_thread(_extract_wait)
                    return middleware

    def delete_entity(self, entity):
        if self._entity_process(entity):
            raise RpcEntityError(endpoint=common.NAME, entity=entity,
                                 reason='Entity is running')
        LOG.info('Try delete %s entity %d' % (self.namespace, entity))
        home = self.entity_home(entity)
        try:
            if os.path.exists(home):
                shutil.rmtree(home)
        except Exception:
            LOG.error('delete %s fail' % home)
            raise
        else:
            self._free_ports(entity)
            self.entitys_map.pop(entity, None)
            self.konwn_appentitys.pop(entity, None)
            systemutils.drop_user(self.entity_user(entity))

    def extract_entity_file(self, entity, objtype, appfile, timeout, exclude=None):
        dst = self.apppath(entity)
        # 异步解压
        if systemutils.POSIX:
            def prefunc():
                systemutils.drop_privileges(self.entity_user(entity), self.entity_group(entity))
                umask()
        else:
            prefunc = None
        waiter = zlibutils.async_extract(src=appfile, dst=dst,
                                         exclude=exclude, timeout=timeout,
                                         native=False, prefunc=prefunc)
        # 解压完成速度过快, 检查是否有错, 没有报错说明解压完成
        if waiter.finished:
            waiter.wait()
        # 返回解压waiter对象
        return waiter

    def start_entity(self, entity, **kwargs):
        pids = kwargs.get('pids')
        objtype = self._objtype(entity)
        user = self.entity_user(entity)
        group = self.entity_group(entity)
        pwd = self.apppath(entity)
        logfile = os.path.join(self.logpath(entity), '%s.log.start.%d' %
                               (objtype, int(time.time())))
        EXEC = os.path.join(pwd, os.path.join('bin', objtype))
        if not os.path.exists(EXEC):
            raise ValueError('Execute targe %s not exist' % EXEC)
        if not os.access(EXEC, os.X_OK):
            os.chmod(EXEC, 0o744)
        args = [EXEC, ]
        with self.lock(entity):
            if self._entity_process(entity, pids=pids):
                raise RpcEntityError(endpoint=common.NAME, entity=entity,
                                     reason='Entity is running')
            logbakup = self.logbakup(entity)
            logpath = self.logpath(entity)
            if not os.path.exists(logbakup):
                try:
                    os.makedirs(logbakup, mode=0o755)
                except (OSError, IOError):
                    LOG.error('Make path for backup entity log fail')
                finally:
                    systemutils.chown(logbakup, self.entity_user(entity), self.entity_group(entity))
            for _filename in os.listdir(logpath):
                _logfile = os.path.join(logpath, _filename)
                if os.path.isfile(_logfile):
                    try:
                        os.rename(_logfile, os.path.join(logbakup, _filename))
                    except (OSError, IOError):
                        LOG.error('Move log file %s to back up path fail' % _filename)
            for _filename in os.listdir(pwd):
                if re.match(COREPATEN, _filename):
                    _croefile = os.path.join(pwd, _filename)
                    try:
                        os.rename(_croefile, os.path.join(logbakup, _filename))
                    except (OSError, IOError):
                        LOG.error('Move croe file %s to back up path fail' % _filename)
            pid = safe_fork(user=user, group=group)
            if pid == 0:
                ppid = os.fork()
                # fork twice
                if ppid == 0:
                    # 关闭已经打开的文件描述符
                    os.closerange(3, systemutils.MAXFD)
                    os.chdir(pwd)
                    # cor file unlimit
                    unlimit_core()
                    # open file limit
                    open_file_limit(4096)
                    with open(logfile, 'ab') as f:
                        os.dup2(f.fileno(), sys.stdout.fileno())
                        os.dup2(f.fileno(), sys.stderr.fileno())
                        # exec后关闭日志文件描述符
                        systemutils.set_cloexec_flag(f.fileno())
                        # 设置环境变量
                        environment = {'LD_LIBRARY_PATH': os.path.join(pwd, 'bin'),  # 小陈的so放在bin目录中
                                       'GOTRACEBACK': 'crash'}  # 允许go调用c崩溃时生成core
                        try:
                            os.execve(EXEC, args, environment)
                        except (OSError, IOError) as e:
                            sys.stderr.write('exec: ' + ' '.join(args) + '\n')
                            sys.stderr.write('environment: ' + str(environment) + '\n')
                            sys.stderr.write(str(e) + '\n')
                            os._exit(1)
                else:
                    os._exit(0)
            else:
                posix.wait(pid)

    def stop_entity(self, entity, pids=None, kill=False):
        with self.lock(entity):
            p = self._entity_process(entity, pids)
            if not p:
                return
            # 程序默认使用SIGINT停服兼容windows和linux
            sig = signal.SIGINT if not kill else signal.SIGKILL
            os.kill(p.pid, sig)
            eventlet.sleep(1)

    def rpc_stoped(self, ctxt, entity, **kwargs):
        timeout = count_timeout(ctxt, kwargs)
        with self.lock(entity, timeout):
            if entity not in set(self.entitys):
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt, result='entity not exist')
            if self._entity_process(entity):
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  ctxt=ctxt,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  result='running')
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              ctxt=ctxt,
                                              resultcode=manager_common.RESULT_SUCCESS,
                                              result='not running')

    def rpc_create_entity(self, ctxt, entity, **kwargs):
        timeout = count_timeout(ctxt, kwargs)
        ports = kwargs.pop('ports', None)
        chiefs = kwargs.pop('chiefs', None)
        objtype = kwargs.pop('objtype')
        databases = kwargs.pop('databases')
        appfile = kwargs.pop(common.APPFILE)
        entity = int(entity)

        try:
            middleware = self._create_entity(entity, objtype, appfile, databases, timeout, ports)
        except NoFileFound:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='create %s.%d fail, appfile not find' % (objtype, entity))

        def _config_wait():
            overtime = int(time.time()) + timeout
            while (entity not in self.konwn_appentitys or middleware.waiter is not None):
                if int(time.time()) > overtime:
                    break
                eventlet.sleep(0.1)
            success = True
            if entity not in self.konwn_appentitys:
                success = False
                LOG.error('Get entity %d from konwn appentity fail' % entity)
                LOG.error('%s' % str(chiefs))
                LOG.error('%s' % str(databases))

            if middleware.waiter is not None:
                success = False
                try:
                    middleware.waiter.stop()
                except Exception as e:
                    LOG.error('Stop waiter catch error %s' % e.__class__.__name__)
                finally:
                    middleware.waiter = None
                LOG.error('Wait extract overtime')
            if not success:
                return
            opentime = self.konwn_appentitys[entity].get('opentime')
            self.flush_config(entity, middleware.databases, opentime, chiefs)

        # 生成配置文件
        threadpool.add_thread(_config_wait)

        result = 'create %s success' % objtype
        return CreateResult(agent_id=self.manager.agent_id, ctxt=ctxt,
                            resultcode=manager_common.RESULT_SUCCESS,
                            result=result,
                            connection=self.manager.local_ip,
                            ports=self._get_ports(entity),
                            databases=middleware.databases)

    def rpc_post_create_entity(self, ctxt, entity, **kwargs):
        LOG.info('Get post create %d command with %s' % (entity, str(kwargs)))
        try:
            self.konwn_appentitys.setdefault(entity, dict(objtype=kwargs.pop('objtype'),
                                                          # group_id=kwargs.pop('group_id'),
                                                          status=kwargs.pop('status'),
                                                          areas=kwargs.pop('areas'),
                                                          opentime=kwargs.pop('opentime', None),
                                                          started=False,
                                                          pid=None))
        except KeyError:
            LOG.error('Fail setdefault for entity by KeyError')

    def rpc_reset_entity(self, ctxt, entity, appfile,
                         databases, chiefs, **kwargs):
        timeout = count_timeout(ctxt, kwargs)
        entity = int(entity)
        _start = time.time()
        with self.lock(entity):
            if self._entity_process(entity):
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  ctxt=ctxt,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  result='entity is running, can not reset')
            objtype = self.konwn_appentitys[entity].get('objtype')
            if appfile:
                try:
                    localfile = self.filemanager.find(appfile)
                except NoFileFound:
                    return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                      resultcode=manager_common.RESULT_ERROR,
                                                      ctxt=ctxt,
                                                      result='reset %s.%d fail, appfile not find' % (objtype, entity))
                appfile = localfile.path
                gfile.check(objtype, appfile)
                if not os.path.exists(self.entity_home(entity)):
                    LOG.warning('Entity is full reset!')
                    with self._prepare_entity_path(entity):
                        bakpath = self.bakpath(entity)
                        os.makedirs(bakpath, mode=0o755)
                        systemutils.chown(bakpath, self.entity_user(entity), self.entity_group(entity))
                apppath = self.apppath(entity)
                cfile = self._objconf(entity, objtype)
                confdir = os.path.split(cfile)[0]
                oldcf = None
                if os.path.exists(cfile):
                    LOG.debug('Read old config from %s' % cfile)
                    with open(cfile, 'rb') as f:
                        oldcf = json.load(f)
                LOG.info('Remove path %s' % apppath)
                if os.path.exists(apppath):
                    shutil.rmtree(apppath)
                os.makedirs(apppath, 0o755)
                systemutils.chown(apppath, self.entity_user(entity), self.entity_group(entity))
                if not os.path.exists(confdir):
                    os.makedirs(confdir, mode=0o755)
                    systemutils.chown(confdir, self.entity_user(entity), self.entity_group(entity))
                if oldcf:
                    LOG.debug('Write back old config from %s' % cfile)
                    os.makedirs(confdir, 0o755)
                    systemutils.chown(confdir, self.entity_user(entity), self.entity_group(entity))
                    with open(cfile, 'wb') as f:
                        json.dump(oldcf, f, indent=4, ensure_ascii=False)
                        f.write('\n')
                used = time.time() - _start
                timeout -= used
                waiter = self.extract_entity_file(entity, objtype, appfile, timeout)
                waiter.wait()
            self.flush_config(entity, databases=databases,
                              opentime=kwargs.get('opentime'), chiefs=chiefs)

        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          resultcode=manager_common.RESULT_SUCCESS,
                                          result='entity is reset finish')

    def rpc_delete_entity(self, ctxt, entity, **kwargs):
        entity = int(entity)
        token = kwargs.pop('token')
        timeout = count_timeout(ctxt, kwargs)
        if entity not in set(self.entitys):
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt, result='delete fail, entity not exist')
        while self.frozen:
            if timeout < 1:
                raise RpcTargetLockException(self.namespace, str(entity), 'endpoint locked')
            eventlet.sleep(1)
            timeout -= 1
        timeout = min(1, timeout)
        with self.lock(entity, timeout):
            if entity not in set(self.entitys):
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt, result='delete fail, entity not exist')
            if token != self._entity_token(entity):
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt, result='delete fail, token error')
            try:
                self.delete_entity(entity)
                resultcode = manager_common.RESULT_SUCCESS
                result = 'delete %d success' % entity
            except Exception as e:
                resultcode = manager_common.RESULT_ERROR
                result = 'delete %d fail with %s:%s' % (entity, e.__class__.__name__,
                                                        str(e.message) if hasattr(e, 'message') else 'unknown err msg')
        self.manager.change_performance()
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          resultcode=resultcode,
                                          result=result)

    def rpc_opentime_entity(self, ctxt, entity, opentime):
        if entity not in self.entitys:
            raise RpcCtxtException('Entity %d not exist' % entity)
        objtype = self.konwn_appentitys[entity].get('objtype')
        if objtype != common.GAMESERVER:
            raise ValueError('%s has no opentime conf' % object)
        with self.lock(entity=entity):
            self.konwn_appentitys[entity]['opentime'] = opentime
            self.flush_config(entity, opentime=opentime)
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          resultcode=manager_common.RESULT_SUCCESS,
                                          ctxt=ctxt,
                                          result='change entity opentime success')

    def rpc_change_entity_area(self, ctxt, entity, area_id, areaname, show_id, **kwargs):
        entity = int(entity)
        timeout = count_timeout(ctxt, kwargs)
        while self.frozen:
            if timeout < 1:
                raise RpcTargetLockException(self.namespace, str(entity), 'endpoint locked')
            eventlet.sleep(1)
            timeout -= 1
        timeout = min(1, timeout)
        running = False
        with self.lock(entity, timeout):
            if entity not in set(self.entitys):
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt, result='change entity area fail, entity not exist')
            if self._entity_process(entity):
                LOG.warning('Change area will not flush config')
                running = True
            areas = self.konwn_appentitys[entity].get('areas')
            for area in areas:
                if area.get('area_id') == area_id:
                    area['areaname'] = areaname
                    area['show_id'] = show_id
            if not running:
                self.flush_config(entity)
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          result='change entity area info success')

    def rpc_change_status(self, ctxt, entity, status, **kwargs):
        if entity not in set(self.entitys):
            LOG.error('entity not found, can not change status')
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='change entity %d status fail, not exit' % entity)
        self.konwn_appentitys[entity].update({'status': status})
        objtype = self._objtype(entity)
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          resultcode=manager_common.RESULT_SUCCESS,
                                          ctxt=ctxt,
                                          result='change entity %s.%d status success' % (objtype, entity))

    def rpc_start_entitys(self, ctxt, entitys, **kwargs):
        timeout = count_timeout(ctxt, kwargs)
        overtime = timeout + time.time()
        entitys = argutils.map_to_int(entitys) & set(self.entitys)
        if not entitys:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='start entitys fail, no entitys found')
        details = []
        # 启动前进程快照
        proc_snapshot_before = utils.find_process()
        formater = AsyncActionResult('start', self.konwn_appentitys)

        def safe_wapper(__entity):
            try:
                self.start_entity(__entity, pids=proc_snapshot_before)
                details.append(formater(__entity, manager_common.RESULT_SUCCESS))
            except RpcTargetLockException as e:
                details.append(formater(__entity, manager_common.RESULT_ERROR,
                                        'start entity %d fail, %s' % (__entity, e.message)))
            except Exception as e:
                details.append(formater(__entity, manager_common.RESULT_ERROR,
                                        'start entity %d fail: %s' % (__entity, e.__class__.__name__)))
                LOG.exception('Start entity %d fail' % __entity)

        for entity in entitys:
            status = self.konwn_appentitys[entity].get('status')
            if status == common.OK:
                eventlet.spawn_n(safe_wapper, entity)
            else:
                details.append(formater(entity, manager_common.RESULT_ERROR,
                                        'start entity %d fail, status %s' % str(status)))
        while len(details) < len(entitys):
            eventlet.sleep(0.5)
            if int(time.time()) > overtime:
                break

        responsed_entitys = set()
        # 启动后进程快照
        proc_snapshot_after = utils.find_process()
        # 确认启动成功
        for detail in details:
            entity = detail.get('detail_id')
            # 确认entity进程
            if not self._entity_process(entity, proc_snapshot_after):
                detail.update(formater(entity, manager_common.RESULT_ERROR,
                                       'start entity %d fail, process not exist after start' % entity))
            else:
                self.konwn_appentitys[entity]['started'] = True
                # 写入PID
                results = detail['result'].split('|')
                results[1] = str(self.konwn_appentitys[entity]['pid'])
                detail['result'] = '|'.join(results)
            responsed_entitys.add(entity)

        for no_response_entity in (entitys - responsed_entitys):
            details.append(formater(no_response_entity, manager_common.RESULT_ERROR,
                                    'start entity %d overtime, result unkonwn'))

        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          result='Start entity end', details=details)

    def rpc_stop_entitys(self, ctxt, entitys, **kwargs):
        timeout = count_timeout(ctxt, kwargs)
        kill = kwargs.get('kill')
        overtime = timeout + time.time()
        entitys = argutils.map_to_int(entitys) & set(self.entitys)
        if not entitys:
            # stop process with signal.SIGINT
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='stop entitys fail, no entitys found')
        for entity in entitys:
            self.konwn_appentitys[entity]['started'] = False
        delay = kwargs.get('delay')
        if delay:
            eventlet.sleep(min(delay, 65))
        details = []
        # 停止前进程快照
        proc_snapshot_before = utils.find_process()
        formater = AsyncActionResult('stop', self.konwn_appentitys)

        def safe_wapper(__entity):
            try:
                if delay and not kill:
                    with self.lock(__entity):
                        if self._entity_process(__entity, proc_snapshot_before):
                            details.append(formater(__entity, manager_common.RESULT_ERROR,
                                                    'entity %d still running, stop fail' % __entity))
                        else:
                            details.append(formater(__entity, manager_common.RESULT_SUCCESS))
                else:
                    self.stop_entity(__entity, pids=proc_snapshot_before, kill=kill)
                    details.append(formater(__entity, manager_common.RESULT_SUCCESS))
            except RpcTargetLockException as e:
                details.append(formater(__entity, manager_common.RESULT_ERROR,
                                        'stop entity %d fail, %s' % (__entity, e.message)))
            except Exception as e:
                details.append(formater(__entity, manager_common.RESULT_ERROR,
                                        'stop entity %d fail: %s' % (__entity, e.__class__.__name__)))
                LOG.exception('stop entity %d fail' % __entity)

        for entity in entitys:
            eventlet.spawn_n(safe_wapper, entity)

        while len(details) < len(entitys):
            eventlet.sleep(0.5)
            if int(time.time()) > overtime:
                break

        responsed_entitys = set()
        # 停止后进程快照
        proc_snapshot_after = utils.find_process()
        for detail in details:
            entity = detail.get('detail_id')
            # 确认entity进程
            if not self._entity_process(entity, proc_snapshot_after):
                results = detail['result'].split('|')
                results[1] = 'N/A'
                detail['result'] = '|'.join(results)
            else:
                detail.update(formater(entity, manager_common.RESULT_ERROR,
                                       'stop entity %d fail, process still exist' % entity))
            responsed_entitys.add(entity)

        for no_response_entity in (entitys - responsed_entitys):
            details.append(formater(no_response_entity, manager_common.RESULT_ERROR,
                                    'stop entity %d overtime, result unkonwn'))

        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          result='Stop entitys end', details=details)

    def rpc_status_entitys(self, ctxt, entitys, **kwargs):
        entitys = argutils.map_to_int(entitys) & set(self.entitys)
        if not entitys:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='status entitys fail, no entitys found')
        details = []
        proc_snapshot = utils.find_process()
        formater = AsyncActionResult('status', self.konwn_appentitys)
        for entity in entitys:
            self._entity_process(entity, proc_snapshot)
            if self._objtype(entity) == common.GAMESERVER:
                msg = self._entity_version(entity)
            else:
                msg = None
            details.append(formater(entity, manager_common.RESULT_SUCCESS, msg))
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          resultcode=manager_common.RESULT_SUCCESS,
                                          result='Get entity status success', details=details)

    def rpc_upgrade_entitys(self, ctxt, entitys, **kwargs):
        entitys = argutils.map_to_int(entitys) & set(self.entitys)
        if not entitys:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='upgrade entity fail, no entitys found')
        timeline = int(kwargs.get('timeline', 0))
        objfiles = kwargs.get('objfiles')
        objtype = kwargs.get('objtype')
        # 校验objtype是否一致
        for entity in entitys:
            if self._objtype(entity) != objtype:
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt,
                                                  result='upgrade entity %d not %s' % (entity, objtype))
        details = []
        formater = AsyncActionResult('upgrade', self.konwn_appentitys)
        with self.locks(entitys):
            # 启动前进程快照
            proc_snapshot_before = utils.find_process()
            for entity in entitys:
                if self._entity_process(entity, proc_snapshot_before):
                    for __entity in entitys:
                        details.append(formater(__entity, manager_common.RESULT_ERROR,
                                                'upgrade entitys not executed, some entity is running'))
                    break
                status = self.konwn_appentitys[entity].get('status')
                if status != common.OK:
                    for __entity in entitys:
                        details.append(formater(__entity, manager_common.RESULT_ERROR,
                                                'upgrade entitys not executed, some entity status not ok'))
                    break
            if details:
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  ctxt=ctxt,
                                                  result='upgrade entity fail, check status fail',
                                                  details=details)
            try:
                middlewares, e = taskupgrade.upgrade_entitys(self, objtype, objfiles, entitys, timeline)
            except Exception as e:
                if hasattr(e, 'message'):
                    msg = e.message
                else:
                    msg = 'prepare upgrade taskflow fail'
                if LOG.isEnabledFor(logging.DEBUG):
                    LOG.exception(msg)
                else:
                    LOG.error('prepare upgrade fail, %s %s' % (e.__class__.__name__, str(e)))
                for entity in entitys:
                    details.append(formater(entity, manager_common.RESULT_ERROR,
                                            'upgrade entity %d not executed' % entity))
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  ctxt=ctxt,
                                                  details=details,
                                                  result='upgrade %s entitys fail, %s' % (objtype, msg))

        for middleware in middlewares:
            if middleware.success:
                details.append(formater(middleware.entity, manager_common.RESULT_SUCCESS))
            elif middleware.notexecuted:
                details.append(formater(middleware.entity, manager_common.RESULT_ERROR,
                                        'upgrade entity %d not executed' % middleware.entity))
            else:
                details.append(formater(middleware.entity, manager_common.RESULT_ERROR,
                                        'upgrade entity %d fail, check agent log for more' % middleware.entity))
                LOG.debug('%s.%d %s', (objtype, middleware.entity, str(middleware)))
        if e:
            if hasattr(e, 'message') and e.message:
                msg = e.message
            else:
                msg = 'Task execute fail by %s' % e.__class__.__name__
            result = 'upgrade %s entitys fail, %s' % (objtype, msg)
        else:
            result = 'upgrade %s entitys finish' % objtype
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          details=details,
                                          result=result)

    def rpc_flushconfig_entitys(self, ctxt, entitys, **kwargs):
        entitys = argutils.map_to_int(entitys) & set(self.entitys)
        force = kwargs.pop('force', False)
        if not entitys:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='upgrade entity fail, no entitys found')
        details = []
        formater = AsyncActionResult('flushconfig', self.konwn_appentitys)
        # 启动前进程快照
        proc_snapshot_before = utils.find_process()
        with self.locks(entitys):
            for entity in entitys:
                if not force and self._entity_process(entity, proc_snapshot_before):
                    for __entity in entitys:
                        details.append(formater(__entity, manager_common.RESULT_ERROR,
                                                'flushconfig entity not executed, some entity is running'))
                    return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                      resultcode=manager_common.RESULT_ERROR,
                                                      ctxt=ctxt,
                                                      result='flushconfig entity fail, entity %d running' % entity,
                                                      details=details)
            for entity in entitys:
                status = self.konwn_appentitys[entity].get('status')
                if status != common.OK:
                    details.append(formater(entity, manager_common.RESULT_ERROR,
                                            'flush entity %d fail,  status not ok'))
                    continue
                try:
                    self.flush_config(entity, opentime=kwargs.get('opentime'),
                                      chiefs=kwargs.get('chiefs'))
                    details.append(formater(entity, manager_common.RESULT_SUCCESS))
                except Exception as e:
                    details.append(formater(entity, manager_common.RESULT_ERROR,
                                            'flushconfig entity %d fail: %s' % (entity, e.__class__.__name__)))

        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          result='Flush entitys config end', details=details)

    def rpc_hotfix_entitys(self, ctxt, entitys, **kwargs):
        entitys = argutils.map_to_int(entitys) & set(self.entitys)
        if not entitys:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='hotfix entity fail, no entitys found')
        timeline = int(kwargs.get('timeline', 0))
        appfile = kwargs.get('appfile')
        objtype = kwargs.get('objtype')
        if objtype != common.GAMESERVER:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='Objtype not %s' % common.GAMESERVER)
        # 校验objtype是否一致
        for entity in entitys:
            if self._objtype(entity) != objtype:
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt,
                                                  result='hotfix entity %d not %s' % (entity, objtype))
        details = []
        formater = AsyncActionResult('hotfix', self.konwn_appentitys)
        with self.locks(entitys):
            for entity in entitys:
                status = self.konwn_appentitys[entity].get('status')
                if status != common.OK:
                    for __entity in entitys:
                        details.append(formater(__entity, manager_common.RESULT_ERROR,
                                                'hotfix entitys not executed, some entity status not ok'))
                    break
            if details:
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  ctxt=ctxt,
                                                  result='hotfix entity fail, check status fail',
                                                  details=details)
            try:
                middlewares, e = taskhotfix.hotfix_entitys(self, objtype, appfile, entitys, timeline)
            except Exception as e:
                if hasattr(e, 'message'):
                    msg = e.message
                else:
                    msg = 'prepare hotfix taskflow fail'
                if LOG.isEnabledFor(logging.DEBUG):
                    LOG.exception(msg)
                else:
                    LOG.error('prepare hotfix fail, %s %s' % (e.__class__.__name__, str(e)))
                for entity in entitys:
                    details.append(formater(entity, manager_common.RESULT_ERROR,
                                            'hotfix entity %d not executed' % entity))
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  ctxt=ctxt,
                                                  details=details,
                                                  result='hotfix %s entitys fail, %s' % (objtype, msg))

        for middleware in middlewares:
            if middleware.success:
                details.append(formater(middleware.entity, manager_common.RESULT_SUCCESS))
            elif middleware.notexecuted:
                details.append(formater(middleware.entity, manager_common.RESULT_ERROR,
                                        'hotfix entity %d not executed'))
            else:
                details.append(formater(middleware.entity, manager_common.RESULT_ERROR,
                                        'hotfix entity %d fail, check agent log for more' % middleware.entity))
                LOG.debug('%s.%d %s', (objtype, middleware.entity, str(middleware)))
        if e:
            if hasattr(e, 'message') and e.message:
                msg = e.message
            else:
                msg = 'Task execute fail by %s' % e.__class__.__name__
            result = 'hotfix %s entitys fail, %s' % (objtype, msg)
        else:
            result = 'hotfix %s entitys finish' % objtype
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          details=details,
                                          result=result)

    def rpc_swallow_entity(self, ctxt, entity, **kwargs):
        if entity not in set(self.entitys):
            LOG.error('entity not found, can not swallow entity')
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='swallow entity %d fail, not exit' % entity)
        objtype = self._objtype(entity)
        if objtype != common.GAMESERVER:
            raise RpcEntityError(endpoint=common.NAME,
                                 entity=entity,
                                 reason='Entity type not %s' % common.GAMESERVER)
        with self.lock(entity):
            if self._entity_process(entity):
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt,
                                                  result='Entity %d is running' % entity)
            status = self.konwn_appentitys[entity].get('status')
            if status != common.UNACTIVE:
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt,
                                                  result='Entity %d status error' % entity)
            self.konwn_appentitys[entity].update({'status': common.SWALLOWING})
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          resultcode=manager_common.RESULT_SUCCESS,
                                          ctxt=ctxt,
                                          result='swallow entity %s.%d success' % (objtype, entity))

    def rpc_swallowed_entity(self, ctxt, entity, **kwargs):
        if entity not in set(self.entitys):
            LOG.error('entity not found, can not swallowd entity')
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='swallowd entity %d fail, not exit' % entity)
        objtype = self._objtype(entity)
        if objtype != common.GAMESERVER:
            raise RpcEntityError(endpoint=common.NAME,
                                 entity=entity,
                                 reason='Entity type not %s' % common.GAMESERVER)
        if self._entity_process(entity):
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='Entity %d is running' % entity)
        status = self.konwn_appentitys[entity].get('status')
        if status != common.SWALLOWING:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='Entity %d status error' % entity)
        self.konwn_appentitys[entity].update({'status': common.DELETED})
        del self.konwn_appentitys[entity]['areas'][:]
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          resultcode=manager_common.RESULT_SUCCESS,
                                          ctxt=ctxt,
                                          result='swallow entity %s.%d success' % (objtype, entity))

    def rpc_merge_entity(self, ctxt, entity, **kwargs):
        uuid = kwargs.pop('uuid')
        entitys = kwargs.pop('entitys')
        ports = kwargs.pop('ports', None)
        opentime = kwargs.pop('opentime')
        chiefs = kwargs.pop('chiefs')
        entity = int(entity)
        appfile = kwargs.pop(common.APPFILE)
        databases = kwargs.pop('databases')
        timeout = count_timeout(ctxt, kwargs)
        # 创建合并后实体
        middleware = self._create_entity(entity, common.GAMESERVER, appfile, databases, timeout, ports)

        def _merge_process():
            with self.lock(entity):
                try:
                    taskmerge.create_merge(self, uuid, entitys, middleware, opentime, chiefs)
                except MergeException as e:
                    LOG.error('First merge fail, %s' % e.message)
                except Exception as e:
                    if LOG.isEnabledFor(logging.DEBUG):
                        LOG.exception('First merge fail')
                    else:
                        LOG.error('First merge, %s %s' % (e.__class__.__name__, str(e)))
                else:
                    if middleware.waiter is not None:
                        try:
                            middleware.waiter.stop()
                        except Exception as e:
                            LOG.error('Stop waiter catch error %s' % e.__class__.__name__)
                        finally:
                            middleware.waiter = None
                        LOG.error('Wait extract overtime')

        # 执行合服工作流
        threadpool.add_thread(_merge_process)

        result = 'merge task is running'
        return CreateResult(agent_id=self.manager.agent_id, ctxt=ctxt,
                            resultcode=manager_common.RESULT_SUCCESS,
                            result=result,
                            connection=self.manager.local_ip,
                            ports=self._get_ports(entity),
                            databases=middleware.databases)

    def rpc_continue_merge(self, ctxt, entity, uuid, databases,
                           **kwargs):

        def wapper():
            with self.lock(entity):
                LOG.info('Get lock success, Try call taskmerge merge_entitys!')
                try:
                    taskmerge.merge_entitys(self, uuid, entity, databases)
                except MergeException as e:
                    LOG.error('Continue merge fail, %s' % e.message)
                except Exception as e:
                    if LOG.isEnabledFor(logging.DEBUG):
                        LOG.exception('Continue merge fail')
                    else:
                        LOG.error('Continue merge, %s %s' % (e.__class__.__name__, str(e)))

        threadpool.add_thread(wapper)

        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          resultcode=manager_common.RESULT_SUCCESS,
                                          ctxt=ctxt,
                                          result='continue merge task %s spawned' % uuid)
