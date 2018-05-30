# -*- coding:utf-8 -*-
import time

from sqlalchemy.orm import joinedload
from sqlalchemy.sql import and_

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import jsonutils
from simpleutil.utils import argutils
from simpleutil.utils import uuidutils
from simpleutil.config import cfg

from simpleservice.ormdb.api import model_query

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

from gogamechen1.models import AppEntity
from gogamechen1.models import GameArea
from gogamechen1.models import MergeTask
from gogamechen1.models import MergeEntity
from gogamechen1.models import PackageEntity

from .base import AppEntityReuestBase

LOG = logging.getLogger(__name__)

port_controller = PortReuest()
entity_controller = EntityReuest()
schema_controller = SchemaReuest()
database_controller = DatabaseReuest()
cdnquote_controller = CdnQuoteRequest()
cdnresource_controller = CdnResourceReuest()

CONF = cfg.CONF


class AppEntityInternalReuest(AppEntityReuestBase):
    """async ext function"""

    MERGEAPPENTITYS = {'type': 'object',
                       'required': [common.APPFILE, 'entitys', 'group_id'],
                       'properties': {
                           'entitys': {'type': 'array',
                                       'items': {'type': 'integer', 'minimum': 1},
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

    def bondto(self, req, entity, body=None):
        """本地记录数据库绑定信息,用于数据绑定失败后重新绑定"""
        body = body or {}
        entity = int(entity)
        databases = body.pop('databases')
        session = endpoint_session()
        with session.begin():
            self._bondto(session, entity, databases)
        return resultutils.results(result='bond entity %d database success' % entity)

    def databases(self, req, objtype, body=None):
        """返回可选数据库列表接口"""
        body = body or {}
        chioces = self._db_chioces(req, objtype, **body)
        return resultutils.results(result='get databases chioces success',
                                   data=chioces)

    def agents(self, req, objtype, body=None):
        """返回可选agent列表接口"""
        body = body or {}
        chioces = self._agent_chioces(req, objtype, **body)
        return resultutils.results(result='get agents chioces success',
                                   data=chioces)

    def entitys(self, req, body=None):
        """批量查询entitys信息接口,内部接口agent启动的时调用,一般由agent端调用"""
        entitys = body.get('entitys')
        if not entitys:
            return resultutils.results(result='not any app entitys found')
        entitys = argutils.map_to_int(entitys)
        session = endpoint_session(readonly=True)
        query = model_query(session, AppEntity, filter=AppEntity.entity.in_(entitys))
        query = query.options(joinedload(AppEntity.areas, innerjoin=False))
        return resultutils.results(result='get app entitys success',
                                   data=[dict(entity=_entity.entity,
                                              group_id=_entity.group_id,
                                              status=_entity.status,
                                              opentime=_entity.opentime,
                                              areas=[dict(area_id=area.area_id,
                                                          areaname=area.areaname)
                                                     for area in _entity.areas],
                                              objtype=_entity.objtype) for _entity in query])

    def merge(self, req, body=None):
        """合服接口,用于合服"""
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
            for appentity in query:
                if appentity.status != common.OK:
                    continue
                if appentity.objtype == common.GMSERVER:
                    gm = appentity
                else:
                    crosss.append(appentity)
            if not gm:
                raise InvalidArgument('Group gm not active?')
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
                LOG.info('Entity controller merge rpc result %s' % str(rpc_result))
                # 插入实体信息
                appentity = AppEntity(entity=mergetd_entity,
                                      agent_id=agent_id,
                                      objtype=common.GAMESERVER,
                                      cross_id=cross.cross_id,
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
                # 修改被合并服的状态
                query.update({'status': common.MERGEING})
        # 添加端口
        threadpool.add_thread(port_controller.unsafe_create,
                              agent_id, common.NAME, mergetd_entity, rpc_result.get('ports'))
        return resultutils.results(result='entitys is mergeing',
                                   data=[dict(uuid=uuid, entitys=entitys, entity=mergetd_entity)])

    def continues(req, uuid, body=None):
        """中途失败的合服任务再次运行"""
        raise NotImplementedError

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
            databases = dict(zip([database['subtype'] for database in appentity.databases],
                                 [dict(database_id=database['database_id'],
                                       host=database['host'],
                                       port=database['port'],
                                       user=database['user'],
                                       passwd=database['passwd'],
                                       character_set=database['character_set']) for database in appentity.databases]))
            areas = [dict(area_id=area.area_id,
                          areaname=area.areaname)
                     for area in appentity.areas]
            if not databases or not areas:
                LOG.error('Entity no areas or databases record')
            with glock.grouplock(group=appentity.group_id):
                # 发送吞噬命令到目标区服agent
                metadata, ports = self._entityinfo(req=req, entity=entity)
                target = targetutils.target_agent_by_string(metadata.get('agent_type'), metadata.get('host'))
                target.namespace = common.NAME
                rpc_ret = rpc.call(target, ctxt={'agents': [appentity.agent_id, ]},
                                   msg={'method': 'swallow',
                                        'args': dict(entity=entity)})
                if not rpc_ret:
                    raise RpcResultError('swallow entity result is None')
                if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
                    raise RpcResultError('swallow entity fail %s' % rpc_ret.get('result'))
            # 修改实体在合服任务中的状态,存储areas以及databases
            _entity.status = common.SWALLOWING
            _entity.areas = jsonutils.dumps(areas)
            _entity.databases = jsonutils.dumps(databases)
            session.flush()
            return resultutils.results(result='swallow entity is success',
                                       data=[dict(databases=databases, areas=areas)])

    def swallowed(self, req, entity, body=None):
        """合服内部接口,一般由agent调用
        用于新实体吞噬旧实体的区服完成后调用
        调用后将设置appentity为deleted状态"""
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
                                   msg={'method': 'swallowed',
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
            _query.update({'entity': _entity.entity})
            session.flush()
            # 更新渠道包含关系
            olds = set()
            news = set()
            _query = model_query(session, PackageEntity, filter=PackageEntity.entity.in_([entity, _entity.entity]))
            for pentity in _query:
                if pentity.entity == entity:
                    olds.add(pentity.package_id)
                else:
                    news.add(pentity.package_id)
            if (olds & news):
                _query = model_query(session, PackageEntity,
                                     filter=and_(PackageEntity.entity == entity,
                                                 PackageEntity.package_id.in_(olds & news)))
                _query.delete()
                session.flush()
            if (olds - news):
                _query = model_query(session, PackageEntity,
                                     filter=and_(PackageEntity.entity == entity,
                                                 PackageEntity.package_id.in_(olds - news)))
                _query.update({'entity': _entity.entity})
                session.flush()
            return resultutils.results(result='swallowed entity is success',
                                       data=[dict(databases=jsonutils.loads_as_bytes(_entity.databases),
                                                  areas=jsonutils.loads_as_bytes(_entity.areas))])

    def spit(self, req, entity, body=None):
        """合服内部接口,用于新实体在失败时候吐出旧实体的区服"""
        raise NotImplementedError
