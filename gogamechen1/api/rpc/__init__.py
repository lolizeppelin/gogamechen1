import os
import time
import shutil
import json
import contextlib
import functools
import eventlet
import psutil

from simpleutil.utils import argutils
from simpleutil.utils import singleton
from simpleutil.utils import systemutils
from simpleutil.log import log as logging
from simpleutil.config import cfg

from simpleutil.utils import zlibutils
from simpleutil.utils.systemutils import posix

from goperation import threadpool
from goperation.utils import safe_fork
from goperation.utils import safe_func_wrapper
from goperation.manager.api import get_http
from goperation.manager import common as manager_common
from goperation.manager.rpc.agent.application.base import AppEndpointBase

from goperation.manager.utils import resultutils
from goperation.manager.utils import validateutils
from goperation.manager.rpc.exceptions import RpcCtxtException
from goperation.manager.rpc.exceptions import RpcTargetLockException


from gogamechen1 import common
from gogamechen1 import utils

from gogamechen1.api import gconfig
from gogamechen1.api import gfile
from gogamechen1.api.client import GogameChen1DBClient
from gogamechen1.api.rpc.config import gameserver_group
from gogamechen1.api.rpc.config import crossserver_group
from gogamechen1.api.rpc.config import gmserver_group
from gogamechen1.api.rpc.config import register_opts

from gogamechen1.api.rpc import taskflow


CONF = cfg.CONF
CONF.register_group(gameserver_group)
CONF.register_group(crossserver_group)
CONF.register_group(gmserver_group)
register_opts()


LOG = logging.getLogger(__name__)


def count_timeout(ctxt, kwargs):
    deadline = ctxt.get('deadline')
    timeout = kwargs.pop('timeout', None)
    if deadline is None:
        return timeout if timeout is not None else 600
    deadline = deadline - int(time.time())
    if timeout is None:
        return deadline
    return min(deadline, timeout)


@singleton.singleton
class Application(AppEndpointBase):

    def __init__(self, manager):
        group = CONF.find_group(common.NAME)
        super(Application, self).__init__(manager, group.name)
        self.client = GogameChen1DBClient(get_http())
        self.delete_tokens = {}
        self.konwn_appentitys = {}

    @property
    def apppathname(self):
        return 'gogame'

    @property
    def logpathname(self):
        return 'log'

    def entity_user(self, entity):
        return 'gogamechen1-%d' % entity

    def entity_group(self, entity):
        return 'gogamechen1'

    def post_start(self):
        super(Application, self).post_start()
        pids = utils.find_process()
        # reflect entity objtype
        if self.entitys:
            LOG.info('Try reflect entity objtype and group info')
            entitymaps = self.client.appentitys(entitys=self.entitys)['data']
            if len(entitymaps) != len(self.entitys):
                raise RuntimeError('Entity count error, miss some entity')
            for entityinfo in entitymaps:
                _entity = entityinfo.get('entity')
                objtype = entityinfo.get('objtype')
                group_id = entityinfo.get('group_id')
                areas = entityinfo.get('areas')
                opentime= entityinfo.get('opentime')
                if _entity in self.konwn_appentitys:
                    raise RuntimeError('App Entity %d Duplicate' % _entity)
                LOG.info('Entity %d type %s, group %d' % (_entity, objtype, group_id))
                self.konwn_appentitys.setdefault(_entity, dict(objtype=objtype, group_id=group_id,
                                                               areas=areas, opentime=opentime,
                                                               pid=None))
        # find entity pid
        for entity in self.entitys:
            _pid = self._find_from_pids(entity, self.konwn_appentitys[entity].get('objtype'),
                                        pids)
            if _pid:
                LOG.info('App entity %d is running at %d' % (entity, _pid))
                self.konwn_appentitys[entity]['pid'] = _pid

    def _esure(self, entity, objtype, username, pwd):
        datadir = False
        runuser = False
        if username == self.entity_user(entity):
            runuser = True
        if pwd == os.path.join(self.apppath(entity), objtype):
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
        for info in pids:
            if self._esure(entity, objtype, info.get('username'), info.get('pwd')):
                return info.get('pid')

    def _objconf(self, entity, objtype):
        return os.path.join(self.apppath(entity), objtype, 'conf', '%s.conf' % objtype)

    @contextlib.contextmanager
    def _allocate_port(self, entity, objtype, ports):
        if isinstance(ports, (int, long, type(None))):
            ports = [ports]
        # if objtype == common.GMSERVER:
        #     if len(ports) == 1:
        #         ports.append(None)
        #     if len(ports) != 2:
        #         raise ValueError('%s need to ports' % common.GMSERVER)
        # else:
        #     if len(ports) > 1:
        #         raise ValueError('Too many ports')
        if len(ports) > 1:
            raise ValueError('Too many ports')
        with self.manager.frozen_ports(common.NAME, entity, ports=ports) as ports:
            yield list(ports)

    def _free_ports(self, entity):
        ports = self.manager.allocked_ports.get(common.NAME)[entity]
        self.manager.free_ports(ports)

    def _get_ports(self, entity):
        return [port for port in self.entitys_map[entity]]

    def _entity_process(self, entity):
        entityinfo = self.konwn_appentitys.get(entity)
        if not entityinfo:
            return
        objtype = entityinfo.get('objtype')
        _pid = entityinfo.get('pid')
        if _pid:
            try:
                p = psutil.Process(pid=_pid)
                if self._esure(entity, objtype, p.username(), p.cwd()):
                    info = dict(pid=p.pid, exe=p.exe(), pwd=p.cwd(), username=p.username())
                    setattr(p, 'info', info)
                    return p
            except psutil.NoSuchProcess:
                _pid = None
        if not _pid:
            _pid = self._find_from_pids(entity, objtype)
        if not _pid:
            self.konwn_appentitys[entity]['pid'] = None
            return None
        try:
            p = psutil.Process(pid=_pid)
            info = dict(pid=p.pid, exe=p.exe(), pwd=p.cwd(), username=p.username())
            setattr(p, 'info', info)
            self.konwn_appentitys[entity]['pid'] = _pid
            return p
        except psutil.NoSuchProcess:
            self.konwn_appentitys[entity]['pid'] = None
            return None


    def flush_config(self, entity, databases=None,
                     opentime=None, chiefs=None):
        eventlet.sleep(0.01)
        objtype = self.konwn_appentitys[entity].get('objtype')
        areas = self.konwn_appentitys[entity].get('areas')
        cfile = self._objconf(entity, objtype)
        posts = self._get_ports(entity)
        databases = gconfig.format_databases(objtype, cfile, databases)
        chiefs = gconfig.format_chiefs(objtype, cfile, chiefs)
        opentime = gconfig.format_opentime(objtype, cfile, opentime)
        confobj = gconfig.make(objtype, self.logpath(entity),
                               self.manager.local_ip, posts,
                               entity, areas,
                               databases, opentime, chiefs)
        LOG.info('Make config for %s.%d success' % (objtype, entity))
        with open(cfile, 'wb') as f:
            json.dump(confobj, f, indent=4)

    def delete_entity(self, entity):
        if self._entity_process(entity):
            raise ValueError('Entity is running')
        LOG.info('Try delete %s entity %d' % (self.namespace, entity))
        home = self.entity_home(entity)
        if os.path.exists(home):
            try:
                shutil.rmtree(home)
            except Exception:
                LOG.exception('delete error')
                raise
            else:
                self._free_ports(entity)
                self.entitys_map.pop(entity, None)
                self.konwn_appentitys.pop(entity, None)
                systemutils.drop_user(self.entity_user(entity))

    def create_entity(self, entity, objtype, objfile, timeout,
                      databases, chiefs):
        timeout = timeout if timeout else 30
        overtime = int(time.time()) + timeout
        wait = zlibutils.async_extract(src=objfile, dst=self.apppath(entity), timeout=timeout,
                                       fork=functools.partial(safe_fork, self.entity_user(entity),
                                                              self.entity_group(entity)))
                                       # exclude=self._exclude(objtype))
        def _postdo():
            wait()
            while entity not in self.konwn_appentitys:
                if int(time.time()) > overtime:
                    LOG.error('Get entity %d from konwn appentity fail, database not bond' % entity)
                    LOG.error('%s' % str(chiefs))
                    LOG.error('%s' % str(databases))
                    return
                eventlet.sleep(0.1)
            opentime = self.konwn_appentitys[entity].get('opentime')
            LOG.info('Try bond database %s' % databases)
            self.client.bondto(entity, databases)
            LOG.info('Try bond database success, flush config')
            self.flush_config(entity, databases, opentime, chiefs)

        threadpool.add_thread(_postdo)

    def start_entity(self, entity, **kwargs):
        objtype = self.konwn_appentitys[entity].get('objtype')
        user = self.entity_user(entity)
        group = self.entity_group(entity)
        pwd = self.apppath(entity)
        logfile = os.path.join(self.logpath(entity), '%s.log.%d' %
                               (objtype, int(time.time())))
        EXEC = os.path.join(pwd, os.path.join('bin', objtype))
        if not os.path.exists(EXEC):
            raise ValueError('Execute targe %s not exist' % EXEC)
        args = [EXEC, ]
        pid = safe_fork(user=user, group=group)
        if pid == 0:
            ppid = os.fork()
            # fork twice
            if ppid == 0:
                os.closerange(3, systemutils.MAXFD)
                os.chdir(pwd)
                with open(logfile, 'ab') as f:
                    os.dup2(f.fileno(), 1)
                    os.dup2(f.fileno(), 2)
                os.execv(EXEC, args)
            else:
                os._exit(0)
        else:
            posix.wait(pid)

    def stop(self, entity):
        pass

    def rpc_create_entity(self, ctxt, entity, **kwargs):
        timeout = count_timeout(ctxt, kwargs)
        objfile = kwargs.pop('objfile')
        ports = kwargs.pop('ports', None)
        chiefs = kwargs.pop('chiefs', None)
        objtype = kwargs.pop('objtype')
        databases = kwargs.pop('databases')
        objfile = self.filemanager.get(objfile, download=False)
        gfile.check(objtype, objfile)

        entity = int(entity)
        with self.lock(entity, timeout=3):
            if entity in self.entitys:
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt,
                                                  result='create %s database fail, entity exist' % entity)
            with self._prepare_entity_path(entity):
                os.makedirs(os.path.split(self._objconf(entity, objtype))[0], mode=0755, exist_ok=True)
                with self._allocate_port(entity, objtype, ports) as ports:
                    middleware = taskflow.create_entity(self, entity, objtype, databases,
                                                        chiefs, objfile, timeout)
                    if not middleware.success:
                        LOG.error('create middleware result %s' % str(middleware))
                        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                          ctxt=ctxt,
                                                          resultcode=manager_common.RESULT_ERROR,
                                                          result=str(middleware))
                    def _port_notity():
                        """notify port bond"""
                        self.client.ports_add(agent_id=self.manager.agent_id,
                                              endpoint=common.NAME, entity=entity, ports=ports)
                    threadpool.add_thread(_port_notity)

        resultcode = manager_common.RESULT_SUCCESS
        result = 'create %s success' % objtype

        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          resultcode=resultcode,
                                          result=result)

    def rpc_post_create_entity(self, ctxt, entity, **kwargs):
        LOG.info('Get post create command with %s' % str(kwargs))
        self.konwn_appentitys.setdefault(entity, dict(objtype=kwargs.pop('objtype'),
                                                      group_id=kwargs.pop('group_id'),
                                                      areas=kwargs.pop('areas'),
                                                      opentime=kwargs.pop('opentime'),
                                                      pid=None))

    def rpc_reset_entity(self, ctxt, entity, **kwargs):
        entity = int(entity)
        pass

    def rpc_delete_entity(self, ctxt, entity, **kwargs):
        entity = int(entity)
        token = kwargs.pop('token')
        timeout = count_timeout(ctxt, kwargs if kwargs else {})
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
                                                  ctxt=ctxt, result='delete database fail, entity not exist')
            if token != self._entity_token(entity):
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt, result='delete database fail, token error')
            try:
                self.delete_entity(entity)
                resultcode = manager_common.RESULT_SUCCESS
                result = 'delete %d success' % entity
            except Exception as e:
                resultcode = manager_common.RESULT_ERROR
                result = 'delete %d fail with %s:%s' % (entity, e.__class__.__name__,
                                                        str(e.message) if hasattr(e, 'message') else 'unknown err msg')
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          resultcode=resultcode,
                                          result=result)

    def rpc_start_entitys(self, ctxt, entitys, **kwargs):
        overtime = count_timeout(ctxt, kwargs) + time.time()
        entitys = argutils.map_to_int(entitys) & set(self.entitys)
        if not entitys:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='start entitys fail, no entitys found')

        details = []
        for entity in entitys:

            def safe_wapper(target_id):
                try:
                    self.start_entity(target_id)
                    details.append(dict(detail_id=target_id,
                                        resultcode=manager_common.RESULT_SUCCESS,
                                        result='start entity %d success' % target_id
                                        ))
                except Exception:
                    details.append(dict(detail_id=target_id,
                                        resultcode=manager_common.RESULT_ERROR,
                                        result='start entity %d fail' % target_id
                                        ))
                    LOG.exception('Start entity %d fail' % target_id)
            eventlet.spawn_n(safe_wapper, entity)

        while len(details) < len(entitys):
            eventlet.sleep(0.5)
            if int(time.time()) >= overtime:
                break
        responsed_entitys = set([detail.get('detail_id') for detail in details])
        for no_response_entity in (entitys - responsed_entitys):
            details.append(dict(detail_id=no_response_entity,
                                resultcode=manager_common.RESULT_ERROR,
                                result='start entity %d overtime, result unkonwn' % no_response_entity
                                ))
        if all([False if detail.get('resultcode') else True for detail in details]):
            resultcode = manager_common.RESULT_SUCCESS
        else:
            resultcode = manager_common.RESULT_NOT_ALL_SUCCESS

        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          resultcode=resultcode,
                                          result='Start entity end')

    def rpc_stop_entitys(self, ctxt, entitys, **kwargs):
        entitys = argutils.map_to_int(entitys) & set(self.entitys)
        if not entitys:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='stop entitys fail, no entitys found')

    def rpc_status_entitys(self, ctxt, entitys, **kwargs):
        entitys = argutils.map_to_int(entitys) & set(self.entitys)
        if not entitys:
            return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                              resultcode=manager_common.RESULT_ERROR,
                                              ctxt=ctxt,
                                              result='status entitys fail, no entitys found')

    def rpc_opentime_entity(self, ctxt, entity, opentime):
        if entity not in self.entitys:
            raise RpcCtxtException('Entity %d not exist' % entity)
        with self.lock(entity=[entity]):
            self.konwn_appentitys[entity]['opentime'] = opentime
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          resultcode=manager_common.RESULT_SUCCESS,
                                          ctxt=ctxt,
                                          result='change entity opentime success')
