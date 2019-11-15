# -*- coding:utf-8 -*-
import inspect
import time
import contextlib
from collections import OrderedDict

from sqlalchemy.sql import and_

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import jsonutils
from simpleutil.utils import argutils
from simpleutil.config import cfg

from simpleservice.ormdb.api import model_query

from goperation import threadpool
from goperation.utils import safe_func_wrapper
from goperation.manager import common as manager_common
from goperation.manager.api import rpcfinishtime
from goperation.manager.utils import resultutils
from goperation.manager.utils import targetutils
from goperation.manager.wsgi.entity.controller import EntityReuest

from gogamechen1 import common
from gogamechen1.api import endpoint_session
from gogamechen1.models import AppEntity

from gogamechen1.api.wsgi.utils import gmurl

from .base import AppEntityReuestBase

LOG = logging.getLogger(__name__)

entity_controller = EntityReuest()

CONF = cfg.CONF


@contextlib.contextmanager
def empty_context(*args, **kwargs):
    yield


class AppEntityAsyncReuest(AppEntityReuestBase):
    """async ext function"""

    OBJFILES = {'type': 'object',
                'properties': {
                    common.APPFILE: {
                        'type': 'object',
                        'required': ['md5', 'timeout'],
                        'properties': {'md5': {'type': 'string', 'format': 'md5',
                                               'description': '更新程序文件所需文件'},
                                       'timeout': {'type': 'integer', 'minimum': 10, 'maxmum': 300,
                                                   'description': '更新超时时间'},
                                       'backup': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                                  'description': '是否更新前备份程序,默认是'},
                                       'revertable': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                                      'description': '程序文件是否可以回滚,默认是'},
                                       'rollback': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                                    'description': '是否连带回滚(回滚前方已经成功的步骤),默认否'},
                                       }},
                    common.DATADB: {
                        'type': 'object',
                        'required': ['md5', 'timeout'],
                        'properties': {
                            'md5': {'type': 'string', 'format': 'md5', 'description': '更新游戏库所需文件'},
                            'timeout': {'type': 'integer', 'minimum': 30, 'maxmum': 1200,
                                        'description': '更新超时时间'},
                            'backup': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                       'description': '是否更新前备份游戏数据库,默认否'},
                            'revertable': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                           'description': '游戏库是否可以回滚,默认否'},
                            'rollback': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                         'description': '是否连带回滚(回滚前方已经成功的步骤),默认否'}}},
                    common.LOGDB: {
                        'type': 'object',
                        'required': ['md5', 'timeout'],
                        'properties': {
                            'md5': {'type': 'string', 'format': 'md5', 'description': '更新日志库所需文件'},
                            'timeout': {'type': 'integer', 'minimum': 30, 'maxmum': 3600,
                                        'description': '更新超时时间'},
                            'backup': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                       'description': '是否更新前备份日志数据库,默认否'},
                            'revertable': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                           'description': '日志库是否可以回滚,默认否'},
                            'rollback': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                         'description': '是否连带回滚(回滚前方已经成功的步骤),默认否'}}},}
                }

    UPGRADE = {'type': 'object',
               'required': ['request_time', 'finishtime', 'objfiles'],
               'properties': {
                   'objfiles': OBJFILES,
                   'request_time': {'type': 'integer', 'description': '异步请求时间'},
                   'timeline': {'type': 'integer', 'description': '异步请求时间'},
                   'finishtime': {'type': 'integer', 'description': '异步请求完成时间'}}
               }

    FLUSH = {'type': 'object',
             'properties': {
                 common.GMSERVER: {'type': 'integer', 'minimum': 0,
                                   'description': 'GM服务器位置更新, 区服专用参数'},
                 common.CROSSSERVER: {'type': 'integer', 'minimum': 0,
                                      'description': '战场服务器位置更新, 区服专用参数'},
                 'opentime': {'type': 'integer', 'minimum': 0,
                              'description': '游戏服开服时间, 区服专用参数'},
                 'force': {'type': 'boolean',
                           'description': '忽略运行状态'}}
             }

    HOTFIX = {'type': 'object',
              'required': [common.APPFILE],
              'properties': {
                  common.APPFILE: {
                      'type': 'object',
                      'required': ['md5', 'timeout'],
                      'properties': {'md5': {'type': 'string', 'format': 'md5',
                                             'description': '更新程序文件所需文件'},
                                     'timeout': {'type': 'integer', 'minimum': 10, 'maxmum': 300,
                                                 'description': '更新超时时间'},
                                     'backup': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                                'description': '是否更新前备份程序,默认否'},
                                     'revertable': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                                    'description': '程序文件是否可以回滚,默认是'},
                                     'rollback': {'oneOf': [{'type': 'boolean'}, {'type': 'null'}],
                                                  'description': '是否连带回滚(回滚前方已经成功的步骤),默认否'},
                                     'stream': {'oneOf': [{'type': 'string', "minLength": 6, "maxLength": 40000},
                                                          {'type': 'null'}],
                                                'description': '直接以stream流文件发送文件'},
                                     }}}
              }

    def _async_bluck_rpc(self, action, group_id, objtype, entity, body=None, context=None):
        caller = inspect.stack()[0][3]
        body = body or {}
        group_id = int(group_id)

        context = context or empty_context

        if entity == 'all':
            entitys = 'all'
        else:
            entitys = argutils.map_to_int(entity)
        asyncrequest = self.create_asyncrequest(body)
        target = targetutils.target_endpoint(common.NAME)
        session = endpoint_session(readonly=True)
        query = model_query(session, AppEntity, filter=and_(AppEntity.group_id == group_id,
                                                            AppEntity.objtype == objtype))
        emaps = dict()

        for _entity in query:
            if _entity.status <= common.DELETED:
                continue
            if _entity.status != common.OK and action != 'stop':
                continue
            emaps.setdefault(_entity.entity, _entity.agent_id)

        if entitys == 'all':
            entitys = emaps.keys()
            agents = set(emaps.values())
        else:
            if entitys - set(emaps.keys()):
                raise InvalidArgument('Some entitys not found or status is not active')
            agents = set()
            for entity in emaps:
                if entity in entitys:
                    agents.add(emaps[entity])

        with context(asyncrequest.request_id, entitys, agents):
            async_ctxt = dict(pre_run=body.pop('pre_run', None),
                              after_run=body.pop('after_run', None),
                              post_run=body.pop('post_run', None))
            rpc_ctxt = {}
            rpc_ctxt.setdefault('agents', agents)
            rpc_method = '%s_entitys' % action
            rpc_args = dict(entitys=list(entitys))
            rpc_args.update(body)

            def wapper():
                self.send_asyncrequest(asyncrequest, target,
                                       rpc_ctxt, rpc_method, rpc_args, async_ctxt)

            threadpool.add_thread(safe_func_wrapper, wapper, LOG)

        return resultutils.results(result='gogamechen1 %s entitys %s spawning' % (objtype, caller),
                                   data=[asyncrequest.to_dict()])

    def start(self, req, group_id, objtype, entity, body=None):
        return self._async_bluck_rpc('start', group_id, objtype, entity, body)

    def stop(self, req, group_id, objtype, entity, body=None):
        """
        kill 强制关闭
        notify 通过gm服务器通知区服关闭
        """
        body = body or {}
        kill = body.get('kill', False)
        notify = body.pop('notify', False)
        if objtype == common.GAMESERVER and notify and not kill:
            message = body.pop('message', '') or ''
            delay = body.pop('delay', 3)
            if delay:
                if not isinstance(delay, (int, long)) or delay < 3:
                    raise InvalidArgument('Delay value error')
                delay = min(delay, 60)
                finishtime = rpcfinishtime()[0] + delay + 5
                body.update({'finishtime': finishtime, 'delay': delay + 5})
            url = gmurl(req, group_id, interface='closegameserver')

            @contextlib.contextmanager
            def context(reqeust_id, entitys, agents):
                pre_run = {'executer': 'http',
                           'ekwargs': {'url': url, 'method': 'POST', 'async': False,
                                       'json': OrderedDict(RealSvrIds=list(entitys), Msg=message, DelayTime=delay)}
                           }
                body.update({'pre_run': pre_run})
                yield

        else:
            context = None
            body.pop('delay', None)

        return self._async_bluck_rpc('stop', group_id, objtype, entity, body, context)

    def status(self, req, group_id, objtype, entity, body=None):
        return self._async_bluck_rpc('status', group_id, objtype, entity, body)

    def upgrade(self, req, group_id, objtype, entity, body=None):
        body = body or {}
        jsonutils.schema_validate(body, self.UPGRADE)
        objfiles = body.get('objfiles')
        if not objfiles:
            raise InvalidArgument('Not objfile found for upgrade')
        request_time = body.get('request_time')
        finishtime = body.get('finishtime')
        timeline = body.get('timeline') or request_time
        runtime = finishtime - request_time
        for subtype in objfiles:
            if subtype not in (common.APPFILE, common.DATADB, common.LOGDB):
                raise InvalidArgument('json schema error')
            objfile = objfiles[subtype]
            if objfile.get('timeout') + request_time > finishtime:
                raise InvalidArgument('%s timeout over finishtime' % subtype)
        body.update({'timeline': timeline,
                     'deadline': finishtime + 3 + (runtime * 2)})
        body.setdefault('objtype', objtype)
        return self._async_bluck_rpc('upgrade', group_id, objtype, entity, body)

    def flushconfig(self, req, group_id, objtype, entity, body=None):
        body = body or {}
        group_id = int(group_id)
        jsonutils.schema_validate(body, self.FLUSH)
        if objtype == common.GAMESERVER:
            gm = body.pop(common.GMSERVER, 0)
            cross = body.pop(common.CROSSSERVER, 0)
            entitys = []
            if gm:
                entitys.append(gm)
            if cross:
                entitys.append(cross)
            entitys = list(set(entitys))
            if entitys:
                chiefs = {}
                session = endpoint_session()
                query = model_query(session, AppEntity,
                                    filter=and_(AppEntity.group_id == group_id,
                                                AppEntity.entity.in_(entitys)))
                gmsvr = crosssvr = None
                for appserver in query:
                    if appserver.group_id != group_id:
                        raise InvalidArgument('Entity group value error')
                    if appserver.objtype == common.GMSERVER:
                        if appserver.entity != gm:
                            raise InvalidArgument('Find %s but entity is %d' % (common.GMSERVER, gm))
                        gmsvr = appserver
                    elif appserver.objtype == common.CROSSSERVER:
                        if appserver.entity != cross:
                            raise InvalidArgument('Find %s but entity is %d' % (common.CROSSSERVER, cross))
                        crosssvr = appserver
                if gm and not gmsvr:
                    raise InvalidArgument('%s.%d can not be found' % (common.GMSERVER, gm))
                if cross and not crosssvr:
                    raise InvalidArgument('%s.%d can not be found' % (common.CROSSSERVER, cross))
                # 获取实体相关服务器信息(端口/ip)
                maps = entity_controller.shows(endpoint=common.NAME, entitys=entitys)
                if gmsvr:
                    chiefs.setdefault(common.GMSERVER,
                                      dict(entity=gmsvr.entity,
                                           ports=maps.get(gmsvr.entity).get('ports'),
                                           local_ip=maps.get(gmsvr.entity).get('metadata').get('local_ip')
                                           ))
                if crosssvr:
                    chiefs.setdefault(common.CROSSSERVER,
                                      dict(entity=crosssvr.entity,
                                           ports=maps.get(crosssvr.entity).get('ports'),
                                           local_ip=maps.get(crosssvr.entity).get('metadata').get('local_ip')
                                           ))
                body.update({'chiefs': chiefs})
        return self._async_bluck_rpc('flushconfig', group_id, objtype, entity, body)

    def hotfix(self, req, group_id, objtype, entity, body=None):
        group_id = int(group_id)
        body = body or {}
        if objtype != common.GAMESERVER:
            raise InvalidArgument('Hotfix just for %s' % common.GAMESERVER)
        jsonutils.schema_validate(body, self.HOTFIX)
        body.setdefault('objtype', objtype)
        url = gmurl(req, group_id, interface='hotupdateconfig?RealSvrIds=0')

        @contextlib.contextmanager
        def context(reqeust_id, entitys, agents):
            post_run = {'executer': 'http',
                        'ekwargs': {'url': url, 'method': 'GET', 'async': False},
                         'condition': 'entitys',
                         'ckwargs': {'all': False,
                                     'operator': '=',
                                     'value': manager_common.RESULT_SUCCESS,
                                     'counter': '>',
                                     'count': 0
                                     }
                        }
            body.update({'post_run': post_run})
            yield

        return self._async_bluck_rpc('hotfix', group_id, objtype, entity, body, context)
