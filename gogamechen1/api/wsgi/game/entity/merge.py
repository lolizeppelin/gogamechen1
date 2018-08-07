# -*- coding:utf-8 -*-
import time

from sqlalchemy.orm import joinedload
from sqlalchemy.sql import and_

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import jsonutils
from simpleutil.utils import uuidutils
from simpleutil.config import cfg

from simpleservice.ormdb.api import model_query
from simpleservice.ormdb.api import model_count_with_key

from goperation import threadpool
from goperation.manager import common as manager_common
from goperation.manager.api import get_client
from goperation.manager.api import rpcfinishtime
from goperation.manager.utils import resultutils
from goperation.manager.utils import targetutils
from goperation.manager.wsgi.port.controller import PortReuest
from goperation.manager.wsgi.entity.controller import EntityReuest
from goperation.manager.wsgi.exceptions import RpcResultError

from gopdb.api.wsgi.controller import SchemaReuest
from gopdb.api.wsgi.controller import DatabaseReuest

from gopcdn.api.wsgi.resource import CdnQuoteRequest
from gopcdn.api.wsgi.resource import CdnResourceReuest

from gogamechen1 import common
from gogamechen1.api import get_gamelock
from gogamechen1.api import endpoint_session
from gogamechen1.api import exceptions

from gogamechen1.models import AppEntity
from gogamechen1.models import GameArea
from gogamechen1.models import MergeTask
from gogamechen1.models import MergeEntity

from .base import AppEntityReuestBase

LOG = logging.getLogger(__name__)

port_controller = PortReuest()
entity_controller = EntityReuest()
schema_controller = SchemaReuest()
database_controller = DatabaseReuest()
cdnquote_controller = CdnQuoteRequest()
cdnresource_controller = CdnResourceReuest()

CONF = cfg.CONF


class AppEntityMergeReuest(AppEntityReuestBase):
    """合服相关代码"""

    MERGEAPPENTITYS = {'type': 'object',
                       'required': [common.APPFILE, 'entitys', 'group_id'],
                       'properties': {
                           'entitys': {'type': 'array',
                                       'items': {'type': 'integer', 'minimum': 2},
                                       'description': '需要合并的实体列表'},
                           common.APPFILE: {'type': 'string', 'format': 'md5',
                                            'description': '程序文件md5'},
                           'agent_id': {'type': 'integer', 'minimum': 1,
                                        'description': '合并后程序运行服务器,不填自动分配'},
                           'zone': {'type': 'string', 'description': '自动分配的安装区域,默认zone为all'},
                           'opentime': {'type': 'integer', 'minimum': 1514736000,
                                        'description': '合并后的开服时间'},
                           'cross_id': {'type': 'integer', 'minimum': 1,
                                        'description': '合并后对应跨服程序的实体id'},
                           'group_id': {'type': 'integer', 'minimum': 1,
                                        'description': '区服所在的组的ID'},
                           'databases': {'type': 'object', 'description': '程序使用的数据库,不填自动分配'}}
                       }

    def merge(self, req, body=None):
        """合服接口,用于合服, 部分代码和create代码一直,未整合"""
        body = body or {}
        jsonutils.schema_validate(body, self.MERGEAPPENTITYS)

        group_id = body.pop('group_id')
        # 需要合并的实体
        entitys = list(set(body.pop('entitys')))
        entitys.sort()

        session = endpoint_session()

        # 安装文件信息
        appfile = body.pop(common.APPFILE)
        # 选择合并后实例运行服务器
        agent_id = self._agentselect(req, common.GAMESERVER, **body)
        # 选择合并后实体数据库
        databases = self._dbselect(req, common.GAMESERVER, **body)
        opentime = body.get('opentime')
        # 合服任务ID
        uuid = uuidutils.generate_uuid()

        # chiefs信息初始化
        query = model_query(session,
                            AppEntity,
                            filter=and_(AppEntity.group_id == group_id,
                                        AppEntity.objtype.in_([common.GMSERVER, common.CROSSSERVER])))
        # 找到同组的gm和战场服
        gm = None
        cross = None
        crosss = []
        # 默认平台识标
        platform = None
        # 锁组
        glock = get_gamelock()
        with glock.grouplock(group_id):
            if model_count_with_key(session, MergeEntity, filter=MergeEntity.entity.in_(entitys)):
                raise InvalidArgument('Target entity merged or in mergeing')
            for appentity in query:
                if appentity.status != common.OK:
                    continue
                if appentity.objtype == common.GMSERVER:
                    gm = appentity
                else:
                    crosss.append(appentity)
            if not gm:
                raise InvalidArgument('Group not exist or gm not active/exist?')
            if not crosss:
                raise InvalidArgument('Group has no cross server?')
            if not body.get('cross_id'):
                cross = crosss[0]
            else:
                for appentity in crosss:
                    if appentity.entity == body.get('cross_id'):
                        cross = appentity
                        break
            if not cross:
                raise InvalidArgument('cross server can not be found?')
            # 获取实体相关服务器信息(端口/ip)
            maps = entity_controller.shows(endpoint=common.NAME, entitys=[gm.entity, cross.entity])
            chiefs = dict()
            # 战场与GM服务器信息
            for chief in (cross, gm):
                chiefmetadata = maps.get(chief.entity).get('metadata')
                ports = maps.get(chief.entity).get('ports')
                if not chiefmetadata:
                    raise InvalidArgument('%s.%d is offline' % (chief.objtype, chief.entity))
                need = common.POSTS_COUNT[chief.objtype]
                if need and len(ports) != need:
                    raise InvalidArgument('%s.%d port count error, '
                                          'find %d, need %d' % (chief.objtype, chief.entity,
                                                                len(ports), need))
                chiefs.setdefault(chief.objtype,
                                  dict(entity=chief.entity,
                                       ports=ports,
                                       local_ip=chiefmetadata.get('local_ip')))

            # 需要合服的实体
            appentitys = []
            query = model_query(session, AppEntity,
                                filter=and_(AppEntity.group_id == group_id, AppEntity.entity.in_(entitys)))
            query = query.options(joinedload(AppEntity.areas, innerjoin=False))
            with session.begin():
                for appentity in query:
                    if appentity.objtype != common.GAMESERVER:
                        raise InvalidArgument('Target entity %d is not %s' % (appentity.entity, common.GAMESERVER))
                    if appentity.status != common.UNACTIVE:
                        raise InvalidArgument('Target entity %d is not unactive' % appentity.entity)
                    if not appentity.areas:
                        raise InvalidArgument('Target entity %d has no area?' % appentity.entity)
                    if appentity.versions:
                        raise InvalidArgument('Traget entity %d version is not None' % appentity.entity)
                    if platform is None:
                        platform = appentity.platform
                    else:
                        # 区服平台不相同, 位操作合并platform
                        platform = platform | appentity.platform
                    appentitys.append(appentity)
                    if not opentime:
                        opentime = appentity.opentime
                if len(appentitys) != len(entitys):
                    raise InvalidArgument('Can not match entitys count')
                # 完整的rpc数据包,准备发送合服命令到agent
                body = dict(appfile=appfile,
                            databases=databases,
                            opentime=opentime,
                            chiefs=chiefs,
                            uuid=uuid,
                            entitys=entitys)
                body.setdefault('finishtime', rpcfinishtime()[0] + 5)
                try:
                    create_result = entity_controller.create(req=req, agent_id=agent_id,
                                                             endpoint=common.NAME, body=body,
                                                             action='merge')['data'][0]
                except RpcResultError as e:
                    LOG.error('Create entity rpc call fail: %s' % e.message)
                    raise InvalidArgument(e.message)
                mergetd_entity = create_result.get('entity')
                rpc_result = create_result.get('notify')
                LOG.info('Merge to entity %d, agent %d' % (mergetd_entity, agent_id))
                LOG.debug('Entity controller merge rpc result %s' % str(rpc_result))
                # 插入实体信息
                appentity = AppEntity(entity=mergetd_entity,
                                      agent_id=agent_id,
                                      group_id=group_id, objtype=common.GAMESERVER,
                                      cross_id=cross.entity,
                                      opentime=opentime,
                                      platform=platform)
                session.add(appentity)
                session.flush()
                # 插入数据库绑定信息
                if rpc_result.get('databases'):
                    self._bondto(session, mergetd_entity, rpc_result.get('databases'))
                else:
                    LOG.error('New entity database miss')
                # 插入合服记录
                mtask = MergeTask(uuid=uuid, entity=mergetd_entity, mergetime=int(time.time()))
                session.add(mtask)
                session.flush()
                for _appentity in appentitys:
                    session.add(MergeEntity(entity=_appentity.entity, uuid=uuid))
                    session.flush()
                # 批量修改被合并服的状态
                query.update({'status': common.MERGEING},
                             synchronize_session=False)
                session.flush()
            port_controller.unsafe_create(agent_id, common.NAME,
                                          mergetd_entity, rpc_result.get('ports'))
            # agent 后续通知
            threadpool.add_thread(entity_controller.post_create_entity,
                                  _appentity.entity, common.NAME, objtype=common.GAMESERVER,
                                  status=common.UNACTIVE,
                                  opentime=opentime,
                                  group_id=group_id, areas=[])
        # 添加端口
        # threadpool.add_thread(port_controller.unsafe_create,
        #                       agent_id, common.NAME, mergetd_entity, rpc_result.get('ports'))
        return resultutils.results(result='entitys is mergeing',
                                   data=[dict(uuid=uuid, entitys=entitys, entity=mergetd_entity)])

    def continues(self, req, uuid, body=None):
        """中途失败的合服任务再次运行"""
        session = endpoint_session()
        query = model_query(session, MergeTask, filter=MergeTask.uuid == uuid)
        query = query.options(joinedload(MergeTask.entitys, innerjoin=False))
        etask = query.one()
        if etask.status == common.MERGEFINISH:
            raise InvalidArgument('Merge task has all ready finished')
        _query = model_query(session, AppEntity, filter=AppEntity.entity == etask.entity)
        _query = _query.options(joinedload(AppEntity.databases, innerjoin=False))
        appentity = _query.one_or_none()
        if not appentity or not appentity.databases or appentity.objtype != common.GAMESERVER:
            LOG.error('Etask entity can not be found or type/database error')
            raise exceptions.MergeException('Etask entity can not be found or type/database error')
        databases = self._database_to_dict(appentity)
        rpc = get_client()
        metadata, ports = self._entityinfo(req=req, entity=appentity.entity)
        target = targetutils.target_agent_by_string(metadata.get('agent_type'), metadata.get('host'))
        target.namespace = common.NAME
        rpc_ret = rpc.call(target, ctxt={'agents': [appentity.agent_id, ]},
                           msg={'method': 'continue_merge',
                                'args': dict(entity=etask.entity, uuid=uuid, databases=databases)})
        if not rpc_ret:
            raise RpcResultError('continue entity result is None')
        if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
            raise RpcResultError('continue entity fail %s' % rpc_ret.get('result'))
        return resultutils.results(result='continue merge task command has been send',
                                   data=[dict(uuid=etask.uuid, entity=etask.entity)])

    def swallow(self, req, entity, body=None):

        """合服内部接口,一般由agent调用
        用于新实体吞噬旧实体的区服和数据库"""
        body = body or {}
        entity = int(entity)
        uuid = body.get('uuid')
        if not uuid:
            raise InvalidArgument('Merger uuid is None')
        session = endpoint_session()
        query = model_query(session, MergeTask, filter=MergeTask.uuid == uuid)
        query = query.options(joinedload(MergeTask.entitys, innerjoin=False))
        glock = get_gamelock()
        rpc = get_client()
        with session.begin():
            etask = query.one_or_none()
            if not etask:
                raise InvalidArgument('Not task exit with %s' % uuid)
            # 新实体不匹配
            if etask.entity != body.get('entity'):
                raise InvalidArgument('New entity not %d' % etask.entity)
            # 找到目标实体
            appentity = None
            for _entity in etask.entitys:
                if _entity.entity == entity:
                    if _entity.status != common.MERGEING:
                        if _entity.status != common.SWALLOWING:
                            raise InvalidArgument('Swallow entity find status error')
                        if not _entity.databases or not _entity.areas:
                            raise InvalidArgument('Entity is swallowing but database or ares is None')
                        LOG.warning('Entit is swallowing, return saved data')
                        return resultutils.results(result='swallow entity is success',
                                                   data=[dict(databases=jsonutils.loads_as_bytes(_entity.databases),
                                                              areas=jsonutils.loads_as_bytes(_entity.areas))])
                    _query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
                    _query = _query.options(joinedload(AppEntity.databases, innerjoin=False))
                    appentity = _query.one_or_none()
                    break
            if not appentity:
                raise InvalidArgument('Can not find app entity?')
            if appentity.objtype != common.GAMESERVER:
                raise InvalidArgument('objtype error, entity not %s' % common.GAMESERVER)
            if appentity.status != common.MERGEING:
                raise InvalidArgument('find status error, when swallowing')
            databases = self._database_to_dict(appentity)
            areas = [area.to_dict()
                     for area in appentity.areas]
            if not databases or not areas:
                LOG.error('Entity no areas or databases record')
                return resultutils.results(result='swallow entity fail, '
                                                  'target entity can not found database or areas',
                                           resultcode=manager_common.RESULT_ERROR)
            with glock.grouplock(group=appentity.group_id):
                # 发送吞噬命令到目标区服agent
                metadata, ports = self._entityinfo(req=req, entity=entity)
                target = targetutils.target_agent_by_string(metadata.get('agent_type'), metadata.get('host'))
                target.namespace = common.NAME
                rpc_ret = rpc.call(target, ctxt={'agents': [appentity.agent_id, ]},
                                   msg={'method': 'swallow_entity',
                                        'args': dict(entity=entity)})
                if not rpc_ret:
                    raise RpcResultError('swallow entity result is None')
                if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
                    raise RpcResultError('swallow entity fail %s' % rpc_ret.get('result'))
            # 修改实体在合服任务中的状态,存储areas以及databases
            appentity.status = common.SWALLOWING
            _entity.status = common.SWALLOWING
            _entity.areas = jsonutils.dumps(areas)
            _entity.databases = jsonutils.dumps(databases)
            session.flush()
            return resultutils.results(result='swallow entity is success',
                                       data=[dict(databases=databases, areas=areas)])

    def swallowed(self, req, entity, body=None):
        """
        合服内部接口,一般由agent调用
        用于新实体吞噬旧实体的区服完成后调用
        调用后将设置appentity为deleted状态
        """
        body = body or {}
        entity = int(entity)
        uuid = body.get('uuid')
        if not uuid:
            raise InvalidArgument('Merger uuid is None')
        session = endpoint_session()
        query = model_query(session, MergeTask, filter=MergeTask.uuid == uuid)
        query = query.options(joinedload(MergeTask.entitys, innerjoin=False))
        glock = get_gamelock()
        rpc = get_client()
        appentity = None
        with session.begin():
            etask = query.one_or_none()
            if not etask:
                raise InvalidArgument('Not task exit with %s' % uuid)
            # 新实体不匹配
            if etask.entity != body.get('entity'):
                raise InvalidArgument('New entity not %d' % etask.entity)
            for _entity in etask.entitys:
                if _entity.entity == entity:
                    if _entity.status != common.SWALLOWING:
                        raise InvalidArgument('Swallowed entity find status error')
                    _query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
                    appentity = _query.one_or_none()
                    break
            if not appentity:
                raise InvalidArgument('Can not find app entity?')
            if appentity.objtype != common.GAMESERVER:
                raise InvalidArgument('objtype error, entity not %s' % common.GAMESERVER)
            if appentity.status != common.SWALLOWING:
                raise InvalidArgument('find status error, when swallowed')

            with glock.grouplock(group=appentity.group_id):
                # 发送吞噬完成命令到目标区服agent
                metadata, ports = self._entityinfo(req=req, entity=entity)
                target = targetutils.target_agent_by_string(metadata.get('agent_type'), metadata.get('host'))
                target.namespace = common.NAME
                rpc_ret = rpc.call(target, ctxt={'agents': [appentity.agent_id, ]},
                                   msg={'method': 'swallowed_entity',
                                        'args': dict(entity=entity)})
                if not rpc_ret:
                    raise RpcResultError('swallowed entity result is None')
                if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
                    raise RpcResultError('swallowed entity fail %s' % rpc_ret.get('result'))
            # appentity状态修改为deleted
            appentity.status = common.DELETED
            # 修改实体在合服任务中的状态
            _entity.status = common.MERGEED
            session.flush()
            # area绑定新实体
            _query = model_query(session, GameArea, filter=GameArea.entity == entity)
            _query.update({'entity': etask.entity})
            session.flush()
            return resultutils.results(result='swallowed entity is success',
                                       data=[dict(databases=jsonutils.loads_as_bytes(_entity.databases),
                                                  areas=jsonutils.loads_as_bytes(_entity.areas))])

    def finish(self, req, uuid, body=None):
        """合服完毕接口"""
        session = endpoint_session()
        query = model_query(session, MergeTask, filter=MergeTask.uuid == uuid)
        query = query.options(joinedload(MergeTask.entitys, innerjoin=False))
        etask = query.one()
        if etask.status == common.MERGEFINISH:
            return resultutils.results(result='swallowed finished',
                                       data=[dict(uuid=etask.uuid,
                                                  entity=etask.entity)])
        for _entity in etask.entitys:
            if _entity.status != common.MERGEED:
                raise InvalidArgument('Entity %d status is not mergeed' % _entity.entity)
        etask.status = common.MERGEFINISH
        session.flush()
        return resultutils.results(result='swallowed finished',
                                   data=[dict(uuid=etask.uuid,
                                              entity=etask.entity)])
