import os
import time
import shutil
import re
import contextlib
import eventlet
import psutil

from collections import namedtuple

from simpleutil.utils import uuidutils
from simpleutil.utils import jsonutils
from simpleutil.utils import singleton
from simpleutil.utils import systemutils
from simpleutil.log import log as logging
from simpleutil.config import cfg

from simpleutil.utils import zlibutils

from simpleservice.loopingcall import IntervalLoopinTask

from goperation import threadpool
from goperation.utils import safe_fork
from goperation.utils import safe_func_wrapper
from goperation.manager.api import get_http
from goperation.manager import common as manager_common
from goperation.manager.rpc.agent.application.base import AppEndpointBase

from goperation.manager.utils import resultutils
from goperation.manager.utils import validateutils
from goperation.manager.rpc.exceptions import RpcTargetLockException


from gogamechen1 import common
from gogamechen1 import utils

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
        return timeout
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
        entitymaps = self.client.show_appentity(impl='local', body=dict(entitys=self.entitys))['data']
        for entityinfo in entitymaps:
            _entity = int(entityinfo.get('entity'))
            objtype = entityinfo.get('objtype')
            if _entity in self.konwn_appentitys:
                raise RuntimeError('App Entity %d Duplicate' % _entity)
            self.konwn_appentitys.setdefault(_entity, dict(objtype=objtype, pid=None))
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
        if isinstance(ports, (int, long)):
            ports = [ports]
        if objtype == common.GAMESERVER:
            if len(ports) == 1:
                ports.append(None)
            if len(ports) != 2:
                raise ValueError('%s need to ports' % common.GMSERVER)
        else:
            if len(ports) > 1:
                raise ValueError('Too many ports')
        with self.manager.frozen_ports(common.NAME, entity, ports=ports) as ports:
            yield list(ports)

    def _free_ports(self, entity):
        ports = self.manager.allocked_ports.get(common.NAME)[entity]
        self.manager.free_ports(self, ports)

    def _get_ports(self, entity):
        return [port for port in self.entitys_map[entity]]

    def _entity_process(self, entity):
        entityinfo = self.konwn_pids.get(entity)
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
            self.konwn_database[entity]['pid'] = None
            return None
        try:
            p = psutil.Process(pid=_pid)
            info = dict(pid=p.pid, exe=p.exe(), pwd=p.cwd(), username=p.username())
            setattr(p, 'info', info)
            self.konwn_database[entity]['pid'] = _pid
            return p
        except psutil.NoSuchProcess:
            self.konwn_database[entity]['pid'] = None
            return None

    def flush_config(self, entity, databases, chiefs):
        eventlet.sleep(0.01)
        posts = self._get_ports(entity)
        objtype = self.konwn_appentitys[entity].get('objtype')
        def _bond():
            self.client.bondto(entity, databases)
        threadpool.add_thread(_bond)

    def delete_entity(self, entity, token):
        if token != self._entity_token(entity):
            raise
        if self._entity_process(entity):
            raise
        LOG.info('Try delete %s entity %d' % (self.namespace, entity))
        home = self.entity_home(entity)
        if os.path.exists(home):
            try:
                shutil.rmtree(home)
            except Exception:
                LOG.exception('delete error')
                raise
            else:
                self._free_port(entity)
                self.entitys_map.pop(entity)

    def create_entity(self, entity, objtype, objfile, timeout):
        with self._prepare_entity_path(entity):
            zlibutils.async_extract(src=objfile, dst=self.apppath(entity), timeout=timeout,
                                    fork=safe_fork(user=self.entity_user(entity),
                                                   group=self.entity_group(entity)),
                                    exclude=self._exclude(objtype))

    def rpc_create_entity(self, ctxt, entity, **kwargs):
        timeout = count_timeout(ctxt, kwargs)
        objfile = kwargs.pop('objfile', None)
        ports = kwargs.pop('ports')
        chiefs = kwargs.pop('chiefs')
        objtype = kwargs.pop('objtype')
        databases = kwargs.pop('databases')
        objfile = self.filemanager.get(objfile, download=False)
        self._check(objfile, objtype)

        entity = int(entity)
        with self.lock(entity, timeout=3):
            if entity in self.entitys:
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt,
                                                  result='create %s database fail, entity exist' % entity)
            with self._prepare_entity_path(entity):
                with self._allocate_port(entity, objtype, ports) as ports:
                    wapper = taskflow.create_entity(self, entity, objtype, databases,
                                                    chiefs, objfile, timeout)
                    threadpool.add_thread(wapper)
                    self.konwn_appentitys.setdefault(entity, dict(objtype=objtype, pid=None))

                    def _port_notity():
                        """notify port bond"""
                        self.client.ports_add(agent_id=self.manager.agent_id,
                                              endpoint=common.NAME, entity=entity, ports=ports)
                    threadpool.add_thread(_port_notity)

        resultcode = manager_common.RESULT_SUCCESS
        result = 'create database success'

        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          resultcode=resultcode,
                                          result=result,
                                          details=[dict(detail_id=entity,
                                                   resultcode=resultcode,
                                                   result='wait database start')])

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
        details = []
        with self.lock(entity, timeout):
            if entity not in set(self.entitys):
                return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                                  resultcode=manager_common.RESULT_ERROR,
                                                  ctxt=ctxt, result='delete database fail, entity not exist')
            try:
                self.delete_entity(entity, token)
                resultcode = manager_common.RESULT_SUCCESS
                result = 'delete %d success' % entity
            except Exception as e:
                resultcode = manager_common.RESULT_ERROR
                result = 'delete %d fail with %s' % (entity, e.__class__.__name__)
        details.append(dict(detail_id=entity,
                            resultcode=resultcode,
                            result=result))
        return resultutils.AgentRpcResult(agent_id=self.manager.agent_id,
                                          ctxt=ctxt,
                                          resultcode=resultcode,
                                          result=result,
                                          details=details)

    def rpc_start_entity(self, ctxt, entity, **kwargs):
        pass

    def rpc_stop_entity(self, ctxt, entity, **kwargs):
        pass

    def rpc_status_entity(self, ctxt, entity, **kwargs):
        p = self._entity_process(entity)