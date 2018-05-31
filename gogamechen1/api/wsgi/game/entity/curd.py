# -*- coding:utf-8 -*-
import six
import functools
import eventlet
from six.moves import zip

from sqlalchemy.orm import joinedload
from sqlalchemy.sql import and_
from sqlalchemy.sql import func

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import jsonutils
from simpleutil.config import cfg

from simpleservice.ormdb.api import model_query
from simpleservice.ormdb.api import model_count_with_key
from simpleservice.ormdb.api import model_max_with_key

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

from gopcdn.api.wsgi.resource import CdnQuoteRequest

from gogamechen1 import common
from gogamechen1.api import get_gamelock
from gogamechen1.api import endpoint_session

from gogamechen1.models import Group
from gogamechen1.models import AppEntity
from gogamechen1.models import GameArea
from gogamechen1.models import AreaDatabase
from gogamechen1.models import Package
from gogamechen1.models import PackageArea

from .base import AppEntityReuestBase

LOG = logging.getLogger(__name__)

port_controller = PortReuest()
entity_controller = EntityReuest()
schema_controller = SchemaReuest()
cdnquote_controller = CdnQuoteRequest()

CONF = cfg.CONF


class AppEntityCURDRequest(AppEntityReuestBase):
    """App entity curd action"""

    CREATEAPPENTITY = {'type': 'object',
                       'required': [common.APPFILE],
                       'properties': {
                           common.APPFILE: {'type': 'string', 'format': 'md5',
                                            'description': '程序文件md5'},
                           'agent_id': {'type': 'integer', 'minimum': 1,
                                        'description': '程序安装的目标机器,不填自动分配'},
                           'zone': {'type': 'string', 'description': '自动分配的安装区域,默认zone为all'},
                           'opentime': {'type': 'integer', 'minimum': 1514736000,
                                        'description': '开服时间, gameserver专用参数'},
                           'cross_id': {'type': 'integer', 'minimum': 1,
                                        'description': '跨服程序的实体id,gameserver专用参数'},
                           'areaname': {'type': 'string', 'description': '区服名称, gameserver专用参数'},
                           'platform': {'type': 'string', 'description': '平台类型, gameserver专用参数'},
                           'packages': {'type': 'array', 'items': {'type': 'integer', 'minimum': 1},
                                        'description': '新建实体对指定包可见'},
                           'databases': {'type': 'object', 'description': '程序使用的数据库,不填自动分配'}}
                       }

    def index(self, req, group_id, objtype, body=None):
        body = body or {}
        group_id = int(group_id)
        order = body.pop('order', None)
        desc = body.pop('desc', False)
        detail = body.pop('detail', False)
        page_num = int(body.pop('page_num', 0))

        session = endpoint_session(readonly=True)
        columns = [AppEntity.entity,
                   AppEntity.group_id,
                   AppEntity.agent_id,
                   AppEntity.areas,
                   AppEntity.opentime,
                   AppEntity.platform,
                   AppEntity.versions,
                   AppEntity.status,
                   AppEntity.objtype]

        def _databases():
            _maps = {}
            if objtype != common.GAMESERVER:
                return _maps
            query = model_query(session, AreaDatabase)
            for _db in query:
                dbinfo = dict(quote_id=_db.quote_id, subtype=_db.subtype, host=_db.host, port=_db.port, )
                try:
                    _maps[_db.entity].append(dbinfo)
                except KeyError:
                    _maps[_db.entity] = [dbinfo, ]
            return _maps

        if detail:
            dth = eventlet.spawn(_databases)

        results = resultutils.bulk_results(session,
                                           model=AppEntity,
                                           columns=columns,
                                           counter=AppEntity.entity,
                                           order=order, desc=desc,
                                           option=joinedload(AppEntity.areas, innerjoin=False)
                                           if objtype == common.GAMESERVER else None,
                                           filter=and_(AppEntity.group_id == group_id,
                                                       AppEntity.objtype == objtype),
                                           page_num=page_num)
        if detail:
            dbmaps = dth.wait()

        if not results['data']:
            return results

        emaps = entity_controller.shows(endpoint=common.NAME,
                                        entitys=[column.get('entity') for column in results['data']])

        for column in results['data']:
            entity = column.get('entity')
            entityinfo = emaps.get(entity)
            if detail:
                try:
                    column['databases'] = dbmaps[entity]
                except KeyError:
                    LOG.error('Entity %d lose database' % entity)
            if column['agent_id'] != entityinfo.get('agent_id'):
                raise RuntimeError('Entity agent id %d not the same as %d' % (column['agent_id'],
                                                                              entityinfo.get('agent_id')))
            column['ports'] = entityinfo.get('ports')
            metadata = entityinfo.get('metadata')
            if metadata:
                local_ip = metadata.get('local_ip')
                external_ips = metadata.get('external_ips')
            else:
                local_ip = external_ips = None
            column['local_ip'] = local_ip
            column['external_ips'] = external_ips
            versions = column.get('versions')
            if versions:
                column['versions'] = jsonutils.loads_as_bytes(versions)
        return results

    def create(self, req, group_id, objtype, body=None):
        body = body or {}
        group_id = int(group_id)
        jsonutils.schema_validate(body, self.CREATEAPPENTITY)
        # 找cross服务, gameserver专用
        cross_id = body.pop('cross_id', None)
        # 开服时间, gameserver专用
        opentime = body.pop('opentime', None)
        # 区服显示民称, gameserver专用
        areaname = body.pop('areaname', None)
        # 平台类型
        platform = body.pop('platform', None)
        packages = set(body.pop('packages', []))
        session = endpoint_session()
        if objtype == common.GAMESERVER:
            platform = common.PlatformTypeMap.get(platform)
            if not areaname or not opentime or not platform:
                raise InvalidArgument('%s need opentime and areaname and platform' % objtype)
            if packages:
                _packages = model_query(session, Package, filter=Package.package_id.in_(packages)).all()
                if len(_packages) != packages:
                    raise InvalidArgument('Package can not be found when create entity')
                for package in _packages:
                    if package.group_id != group_id:
                        raise InvalidArgument('Package group not match when create entity')
                    if not (package.platform & platform):
                        raise InvalidArgument('Pacakge platform not match when create entity')
        # 安装文件信息
        appfile = body.pop(common.APPFILE)
        LOG.info('Try find agent and database for entity')
        # 选择实例运行服务器
        agent_id = self._agentselect(req, objtype, **body)
        # 选择实例运行数据库
        databases = self._dbselect(req, objtype, **body)
        # 校验数据库信息
        if not self._validate_databases(objtype, databases):
            raise InvalidArgument('Miss some database')
        LOG.info('Find agent and database for entity success')
        query = model_query(session, Group, filter=Group.group_id == group_id)
        joins = joinedload(Group.entitys, innerjoin=False)
        joins = joins.joinedload(AppEntity.databases, innerjoin=False)
        query = query.options(joins)
        _group = query.one()
        glock = get_gamelock()
        with glock.grouplock(group_id):
            typemap = {}
            for _entity in _group.entitys:
                # 跳过未激活的实体
                if _entity.status != common.OK:
                    continue
                try:
                    typemap[_entity.objtype].append(_entity)
                except KeyError:
                    typemap[_entity.objtype] = [_entity, ]
            # 前置实体
            chiefs = None
            # 相同类型的实例列表
            same_type_entitys = typemap.get(objtype, [])
            if objtype == common.GMSERVER:
                # GM服务不允许相同实例,必须clean掉所有同组GM服务器
                for _entity in _group.entitys:
                    if _entity.objtype == common.GMSERVER:
                        return resultutils.results(result='create entity fail, %s duplicate in group' % objtype,
                                                   resultcode=manager_common.RESULT_ERROR)
            else:
                # 非gm实体添加需要先找到同组的gm
                try:
                    gm = typemap[common.GMSERVER][0]
                    if gm.status <= common.DELETED:
                        return resultutils.results(result='Create entity fail, gm mark deleted',
                                                   resultcode=manager_common.RESULT_ERROR)
                except KeyError as e:
                    return resultutils.results(result='Create entity fail, can not find GMSERVER: %s' % e.message,
                                               resultcode=manager_common.RESULT_ERROR)
                if objtype == common.GAMESERVER:

                    if model_count_with_key(session, GameArea,
                                            filter=and_(GameArea.group_id == group_id,
                                                        GameArea.areaname == areaname)):
                        return resultutils.results(result='Create entity fail, name exist',
                                                   resultcode=manager_common.RESULT_ERROR)
                    cross = None
                    # 游戏服务器需要在同组中找到cross实例
                    try:
                        crossservers = typemap[common.CROSSSERVER]
                    except KeyError as e:
                        return resultutils.results(result='create entity fail, can not find my chief: %s' % e.message,
                                                   resultcode=manager_common.RESULT_ERROR)
                    # 如果指定了cross实例id
                    if cross_id:
                        # 判断cross实例id是否在当前组中
                        for _cross in crossservers:
                            if cross_id == _cross.entity:
                                cross = _cross
                                break
                    else:
                        # 游戏服没有相同实例,直接使用第一个cross实例
                        if not same_type_entitys:
                            cross = crossservers[0]
                        else:
                            # 统计所有cross实例的引用次数
                            counted = set()
                            counter = dict()
                            for _cross in crossservers:
                                counter.setdefault(_cross.entity, 0)
                            # 查询当前组内所有entity对应的cross_id
                            for _entity in _group.entitys:
                                if _entity.objtype != common.GAMESERVER:
                                    continue
                                if _entity.cross_id in counted:
                                    continue
                                counter[_entity.cross_id] += 1
                            # 选取引用次数最少的cross_id
                            cross_id = sorted(zip(counter.itervalues(), counter.iterkeys()))[0][1]
                            for _cross in crossservers:
                                if cross_id == _cross.entity:
                                    cross = _cross
                                    break
                    if not cross:
                        raise InvalidArgument('cross server can not be found or not active')
                    # 获取实体相关服务器信息(端口/ip)
                    maps = entity_controller.shows(endpoint=common.NAME, entitys=[gm.entity, cross.entity])
                    for v in six.itervalues(maps):
                        if v is None:
                            raise InvalidArgument('Get chiefs info error, agent not online?')
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
                    cross_id = cross.entity
            # 完整的rpc数据包
            body = dict(objtype=objtype,
                        appfile=appfile,
                        databases=databases,
                        chiefs=chiefs)

            with session.begin():
                body.setdefault('finishtime', rpcfinishtime()[0] + 5)
                try:
                    create_result = entity_controller.create(req=req, agent_id=agent_id,
                                                             endpoint=common.NAME, body=body)['data'][0]
                except RpcResultError as e:
                    LOG.error('Create entity rpc call fail: %s' % e.message)
                    raise InvalidArgument(e.message)
                entity = create_result.get('entity')
                rpc_result = create_result.get('notify')
                LOG.info('Entity controller create rpc result %s' % str(rpc_result))
                # 插入实体信息
                appentity = AppEntity(entity=entity,
                                      agent_id=agent_id,
                                      group_id=group_id, objtype=objtype,
                                      cross_id=cross_id,
                                      opentime=opentime,
                                      platform=platform)
                session.add(appentity)
                session.flush()
                if objtype == common.GAMESERVER:
                    area_id = model_max_with_key(GameArea, GameArea.area_id) + 1
                    session.add(GameArea(areaname=areaname.decode('utf-8')
                    if isinstance(areaname, six.binary_type)
                    else areaname,
                                         group_id=_group.group_id,
                                         entity=appentity.entity))
                    session.flush()
                    # 插入渠道包包含列表中
                    if packages:
                        _pquery = session.query(PackageArea.package_id,
                                                func.max(PackageArea.show_id))
                        _pquery = _pquery.group_by(PackageArea.package_id)
                        _pquery.filter(PackageArea.package_id.in_([package.package_id for package in _packages]))
                        for package_area in _pquery:
                            package_id = package_area[0]
                            show_id = package_area[1] + 1
                            session.add(PackageArea(package_id=package_id, area_id=area_id, show_id=show_id))
                            session.flush()
                            packages.pop(package_id)
                        # 没有在PackageArea中找到,插入show id值1
                        if packages:
                            for package_id in packages:
                                session.add(PackageArea(package_id=package_id, area_id=area_id, show_id=1))
                                session.flush()
                # 插入数据库绑定信息
                if rpc_result.get('databases'):
                    self._bondto(session, entity, rpc_result.get('databases'))
                else:
                    LOG.error('New entity database miss')

            _result = dict(entity=entity,
                           objtype=objtype,
                           agent_id=agent_id,
                           connection=rpc_result.get('connection'),
                           ports=rpc_result.get('ports'),
                           databases=rpc_result.get('databases'))

            areas = []
            if objtype == common.GAMESERVER:
                areas = [dict(area_id=area_id, areaname=areaname)]
                _result.setdefault('areas', areas)
                _result.setdefault('cross_id', cross_id)
                _result.setdefault('opentime', opentime)
                _result.setdefault('platform', platform)
                _result.setdefault('packages', [package.package_id for package in _packages])

            # 添加端口
            threadpool.add_thread(port_controller.unsafe_create,
                                  agent_id, common.NAME, entity, rpc_result.get('ports'))
            # agent 后续通知
            threadpool.add_thread(entity_controller.post_create_entity,
                                  entity, common.NAME, objtype=objtype,
                                  status=common.UNACTIVE,
                                  opentime=opentime,
                                  group_id=group_id, areas=areas)
            return resultutils.results(result='create %s entity success' % objtype,
                                       data=[_result, ])

    def show(self, req, group_id, objtype, entity, body=None):
        body = body or {}
        group_id = int(group_id)
        entity = int(entity)
        session = endpoint_session(readonly=True)
        _format = body.get('format') or 'list'
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        query = query.options(joinedload(AppEntity.databases, innerjoin=False))
        _entity = query.one()
        if _entity.objtype != objtype:
            raise InvalidArgument('Entity is not %s' % objtype)
        if _entity.group_id != group_id:
            raise InvalidArgument('Entity group %d not match  %d' % (_entity.group_id, group_id))
        metadata, ports = self._entityinfo(req, entity)
        if _format == 'list':
            databases = []
        else:
            databases = {}
        for database in _entity.databases:
            dbinfo = dict(quote_id=database.quote_id,
                          database_id=database.database_id,
                          host=database.host,
                          port=database.port,
                          ro_user=database.ro_user,
                          ro_passwd=database.ro_passwd,
                          subtype=database.subtype,
                          schema='%s_%s_%s_%d' % (common.NAME, objtype, database.subtype, entity))
            if _format == 'list':
                databases.append(dbinfo)
            else:
                databases[database.subtype] = dbinfo
        return resultutils.results(result='show %s areas success' % objtype,
                                   data=[dict(entity=_entity.entity,
                                              agent_id=_entity.agent_id,
                                              objtype=objtype, group_id=_entity.group_id,
                                              opentime=_entity.opentime,
                                              platform=_entity.platform,
                                              status=_entity.status,
                                              versions=jsonutils.loads_as_bytes(_entity.versions)
                                              if _entity.versions else None,
                                              areas=[dict(area_id=area.area_id,
                                                          areaname=area.areaname.encode('utf-8'),
                                                          ) for area in _entity.areas],
                                              databases=databases,
                                              metadata=metadata, ports=ports)])

    def update(self, req, group_id, objtype, entity, body=None):
        body = body or {}
        group_id = int(group_id)
        entity = int(entity)
        status = body.get('status', common.OK)
        if status not in (common.UNACTIVE, common.OK):
            raise InvalidArgument('Status not in 0, 1, 2')
        session = endpoint_session()
        glock = get_gamelock()
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        if objtype == common.GAMESERVER:
            query = query.options(joinedload(AppEntity.areas, innerjoin=False))
        _entity = query.one()
        if status == _entity.status:
            return resultutils.results(result='%s entity status in same' % objtype)
        if _entity.status != common.UNACTIVE:
            return resultutils.results(resultcode=manager_common.RESULT_ERROR,
                                       result='%s entity is not unactive' % objtype)
        if _entity.objtype != objtype:
            raise InvalidArgument('Objtype not match')
        if _entity.group_id != group_id:
            raise InvalidArgument('Group id not match')
        entityinfo = entity_controller.show(req=req, entity=entity,
                                            endpoint=common.NAME,
                                            body={'ports': False})['data'][0]
        agent_id = entityinfo['agent_id']
        metadata = entityinfo['metadata']
        if not metadata:
            raise InvalidArgument('Agent is off line, can not reset entity')
        rpc = get_client()
        target = targetutils.target_agent_by_string(metadata.get('agent_type'), metadata.get('host'))
        target.namespace = common.NAME
        if objtype == common.GAMESERVER:
            lock = functools.partial(glock.arealock, group=group_id, areas=[area.area_id for area in _entity.areas])
        else:
            lock = functools.partial(glock.grouplock, group=group_id)
        with lock():
            with session.begin():
                _entity.status = status
                session.flush()
                finishtime, timeout = rpcfinishtime()
                rpc_ret = rpc.call(target, ctxt={'finishtime': finishtime, 'agents': [agent_id, ]},
                                   msg={'method': 'change_status',
                                        'args': dict(entity=entity, status=status)},
                                   timeout=timeout)
                if not rpc_ret:
                    raise RpcResultError('change entity sttus result is None')
                if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
                    raise RpcResultError('change entity status fail %s' % rpc_ret.get('result'))
        return resultutils.results(result='%s entity update success' % objtype)

    def delete(self, req, group_id, objtype, entity, body=None):
        """标记删除entity"""
        body = body or {}
        force = body.get('force', False)
        group_id = int(group_id)
        entity = int(entity)
        session = endpoint_session()
        glock = get_gamelock()
        metadata, ports = self._entityinfo(req=req, entity=entity)
        if not metadata:
            raise InvalidArgument('Agent offline, can not delete entity')
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        if objtype == common.GAMESERVER:
            query = query.options(joinedload(AppEntity.areas, innerjoin=False))
        _entity = query.one()
        if _entity.status == common.DELETED:
            return resultutils.results(result='mark %s entity delete success' % objtype,
                                       data=[dict(entity=entity, objtype=objtype,
                                                  ports=ports, metadata=metadata)])
        if _entity.objtype != objtype:
            raise InvalidArgument('Objtype not match')
        if _entity.group_id != group_id:
            raise InvalidArgument('Group id not match')
        target = targetutils.target_agent_by_string(metadata.get('agent_type'), metadata.get('host'))
        target.namespace = common.NAME
        rpc = get_client()
        with glock.grouplock(group=group_id):
            if objtype == common.GMSERVER:
                if model_count_with_key(session, AppEntity, filter=AppEntity.group_id == group_id) > 1:
                    raise InvalidArgument('You must delete other objtype entity before delete gm')
                if model_count_with_key(session, Package, filter=Package.group_id == group_id) > 1:
                    raise InvalidArgument('You must delete other Package before delete gm')
            elif objtype == common.CROSSSERVER:
                if model_count_with_key(session, AppEntity, filter=AppEntity.cross_id == _entity.entity):
                    raise InvalidArgument('Cross server are reflected')
            with session.begin():
                # 确认实体没有运行
                rpc_ret = rpc.call(target, ctxt={'agents': [_entity.agent_id, ]},
                                   msg={'method': 'stoped',
                                        'args': dict(entity=entity)})
                if not rpc_ret:
                    raise RpcResultError('check entity stoped result is None')
                if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
                    raise RpcResultError('check entity fail %s' % rpc_ret.get('result'))
                _entity.status = common.DELETED
                session.flush()
                if objtype == common.GAMESERVER:
                    # 删除所有资源版本引用
                    if _entity.versions:
                        for quote in six.itervalues(jsonutils.loads_as_bytes(_entity.versions)):
                            threadpool.add_thread(cdnquote_controller.delete, req, quote.get('quote_id'))
                    _entity.versions = None
                    session.flush()
                    if _entity.areas:
                        if len(_entity.areas) > 1:
                            raise InvalidArgument('%s areas more then one' % objtype)
                        area = _entity.areas[0]
                        if not force:
                            if _entity.entity == model_max_with_key(session, AppEntity.entity):
                                raise InvalidArgument('entity %d is not the last entity' % entity)
                        session.flush()
                        session.delete(area)
                        session.flush()
                        _query = model_query(session, PackageArea, filter=PackageArea.area_id == area.area_id)
                        _query.delete()
                        session.flush()
                rpc.cast(target, ctxt={'agents': [_entity.agent_id, ]},
                         msg={'method': 'change_status',
                              'args': dict(entity=entity, status=common.DELETED)})
        return resultutils.results(result='mark %s entity delete success' % objtype,
                                   data=[dict(entity=entity, objtype=objtype,
                                              ports=ports, metadata=metadata)])
