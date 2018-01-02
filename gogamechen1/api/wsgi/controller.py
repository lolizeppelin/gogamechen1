# -*- coding:utf-8 -*-
import six
import eventlet
import webob.exc
from six.moves import zip

from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import jsonutils
from simpleutil.utils import argutils
from simpleutil.utils import uuidutils
from simpleutil.utils import singleton

from simpleservice.ormdb.api import model_query
from simpleservice.ormdb.api import model_count_with_key
from simpleservice.ormdb.exceptions import DBDuplicateEntry
from simpleservice.rpc.exceptions import AMQPDestinationNotFound
from simpleservice.rpc.exceptions import MessagingTimeout
from simpleservice.rpc.exceptions import NoSuchMethod

from goperation import threadpool
from goperation.utils import safe_func_wrapper
from goperation.manager import common as manager_common
from goperation.manager.exceptions import CacheStoneError
from goperation.manager.utils import resultutils
from goperation.manager.utils import targetutils
from goperation.manager.wsgi.contorller import BaseContorller
from goperation.manager.wsgi.entity.controller import EntityReuest
from goperation.manager.wsgi.file.controller import FileReuest
from goperation.manager.wsgi.exceptions import RpcPrepareError
from goperation.manager.wsgi.exceptions import RpcResultError

from gopdb import common as dbcommon
from gopdb.api.wsgi.controller import SchemaReuest
from gopdb.api.wsgi.controller import DatabaseReuest

from gogamechen1 import common
from gogamechen1 import utils
from gogamechen1.api import get_gamelock
from gogamechen1.api import endpoint_session

from gogamechen1.models import Group
from gogamechen1.models import AppEntity
from gogamechen1.models import GameArea
from gogamechen1.models import AreaDatabase
from gogamechen1.models import ObjtypeFile


LOG = logging.getLogger(__name__)

FAULT_MAP = {InvalidArgument: webob.exc.HTTPClientError,
             NoSuchMethod: webob.exc.HTTPNotImplemented,
             AMQPDestinationNotFound: webob.exc.HTTPServiceUnavailable,
             MessagingTimeout: webob.exc.HTTPServiceUnavailable,
             RpcResultError: webob.exc.HTTPInternalServerError,
             CacheStoneError: webob.exc.HTTPInternalServerError,
             RpcPrepareError: webob.exc.HTTPInternalServerError,
             NoResultFound: webob.exc.HTTPNotFound,
             MultipleResultsFound: webob.exc.HTTPInternalServerError
             }


entity_controller = EntityReuest()
file_controller = FileReuest()
schema_controller = SchemaReuest()
database_controller = DatabaseReuest()


def areas_map(group_id):
    session = endpoint_session(readonly=True)
    query = model_query(session, GameArea, filter=GameArea.group_id == group_id)
    maps = {}
    for _areas in query:
        try:
            maps[_areas.entity].append(_areas.area_id)
        except KeyError:
            maps[_areas.entity] = [_areas.area_id, ]
    return maps


@singleton.singleton
class ObjtypeFileReuest(BaseContorller):

    def index(self, req, body=None):
        body = body or {}
        order = body.pop('order', None)
        desc = body.pop('desc', False)
        page_num = int(body.pop('page_num', 0))
        session = endpoint_session(readonly=True)
        columns=[ObjtypeFile.uuid,
                 ObjtypeFile.objtype,
                 ObjtypeFile.subtype,
                 ObjtypeFile.version,]

        results = resultutils.bulk_results(session,
                                           model=ObjtypeFile,
                                           columns=columns,
                                           counter=ObjtypeFile.uuid,
                                           order=order, desc=desc,
                                           page_num=page_num)
        return results

    def create(self, req, body=None):
        body = body or {}
        subtype = utils.validate_string(body.pop('subtype', None))
        objtype = body.pop('objtype')
        version = body.pop('version', None)
        session = endpoint_session()
        objtype_file = ObjtypeFile(objtype=objtype, version=version, subtype=subtype)
        with session.begin():
            create_result = file_controller.create(req, body)
            objtype_file.uuid = create_result['data'][0]['uuid']
        return resultutils.results('creat file for %s success' % objtype,
                                   data=[dict(uuid=objtype_file.uuid)])

    def show(self, req, uuid, body=None):
        body = body or {}
        session = endpoint_session(readonly=True)
        query = model_query(session, ObjtypeFile, filter=ObjtypeFile.uuid == uuid)
        objtype_file = query.one()
        show_result = file_controller.show(req, objtype_file.uuid)
        if show_result['resultcode'] != manager_common.RESULT_SUCCESS:
            return resultutils.results('get file of %s fail, %s' % (uuid, show_result.get('result')))
        file_info = show_result['data'][0]
        file_info.setdefault('subtype', objtype_file.subtype)
        file_info.setdefault('objtype', objtype_file.objtype)
        file_info.setdefault('version', objtype_file.version)
        return resultutils.results('get file of %s success' % uuid,
                                   data=[file_info, ])

    def delete(self, req, uuid, body=None):
        body = body or {}
        session = endpoint_session(readonly=True)
        query = model_query(session, ObjtypeFile, filter=ObjtypeFile.uuid == uuid)
        objtype_file = query.one()
        query.delete()
        return file_controller.delete(req, objtype_file.uuid)

    def update(self, req, uuid, body=None):
        raise NotImplementedError

    def find(self, objtype, subtype, version):
        session = endpoint_session(readonly=True)
        query = model_query(session, ObjtypeFile, filter=and_(ObjtypeFile.objtype == objtype,
                                                              ObjtypeFile.subtype == subtype,
                                                              ObjtypeFile.version == version))
        objfile = query.one()
        return objfile['uuid']


objfile_controller = ObjtypeFileReuest()


@singleton.singleton
class GroupReuest(BaseContorller):

    def index(self, req, body=None):
        body = body or {}
        order = body.pop('order', None)
        desc = body.pop('desc', False)
        page_num = int(body.pop('page_num', 0))

        session = endpoint_session(readonly=True)
        columns=[Group.group_id,
                 Group.name,
                 Group.lastarea,
                 Group.desc,
                 Group.areas]

        results = resultutils.bulk_results(session,
                                           model=Group,
                                           columns=columns,
                                           counter=Group.group_id,
                                           order=order, desc=desc,
                                           option=joinedload(Group.areas, innerjoin=False),
                                           page_num=page_num)
        for column in results['data']:

            areas = column.get('areas', [])
            column['areas'] = []
            for area in areas:
                column['areas'].append(area.area_id)

        return results

    def create(self, req, body=None):
        body = body or {}
        session = endpoint_session()
        name = utils.validate_string(body.get('name'))
        desc = body.get('desc')
        _group = Group(name=name, desc=desc)
        session.add(_group)
        try:
            session.flush()
        except DBDuplicateEntry:
            raise InvalidArgument('Group name duplicate')
        return resultutils.results(result='create group success',
                                   data=[dict(group_id=_group.group_id,
                                              name=_group.name,
                                              lastarea=_group.lastarea)])

    def show(self, req, group_id, body=None):
        body = body or {}
        group_id = int(group_id)
        detail = body.get('detail', False)
        session = endpoint_session(readonly=True)
        query = model_query(session, Group, filter=Group.group_id == group_id)
        joins = joinedload(Group.entitys, innerjoin=False)
        if detail:
            joins = joins.joinedload(AppEntity.areas, innerjoin=False)
        query.options(joins)
        _group = query.one()
        group_info = dict(group_id=_group.group_id,
                          name=_group.name,
                          lastarea=_group.lastarea)
        if detail:
            _entitys = {}
            for entity in _group.entitys:
                objtype = entity.objtype
                info = dict(entity=entity.entity)
                if entity.areas:
                    info.setdefault('areas', [dict(area_id=area.area_id, cross_id=area.cross_id)
                                              for area in entity.areas])
                try:
                    _entitys[objtype].append(info)
                except KeyError:
                    _entitys[objtype] = [info, ]
            group_info.setdefault('entitys', _entitys)
        return resultutils.results(result='show group success', data=[group_info, ])

    def update(self, req, group_id, body=None):
        raise NotImplementedError

    def delete(self, req, group_id, body=None):
        body = body or {}
        group_id = int(group_id)
        session = endpoint_session()
        query = model_query(session, Group, filter=Group.group_id == group_id)
        query.options(joinedload(Group.entitys, innerjoin=False))
        _group = query.one()
        if _group.entitys:
            raise
        query.delete()
        return resultutils.results(result='delete group success',
                                   data=[dict(group_id=_group.group_id, name=_group.name)])

    def chiefs(self, req, group_id, body=None):
        body = body or {}
        group_id = int(group_id)
        session = endpoint_session(readonly=True)
        query = model_query(session, Group, filter=and_(Group.group_id == group_id,
                                                        AppEntity.objtype.in_([common.GMSERVER, common.CROSSSERVER])))
        query.options(joinedload(Group.entitys, innerjoin=False))
        _group = query.one()
        chiefs = []
        entitys = set()
        for entity in _group.entitys:
            entitys.add(entity.entity)
        emaps = entity_controller.shows(common.NAME, entitys=entitys, ports=True, metadata=True)
        for entity in _group.entitys:
            entityinfo = emaps.get(entity.entity)
            metadata = entityinfo.get('metadata')
            ports = entityinfo.get('ports')
            chiefs.append(dict(entity=entity.entity,
                               ports=ports,
                               local_ip=metadata.get('local_ip'),
                               external_ips=metadata.get('external_ips'))
                          )
        return resultutils.results(result='get group chiefs success',
                                   data=chiefs)

    def maps(self, req, group_id, body=None):
        body = body or {}
        group_id = int(group_id)
        maps = areas_map(group_id)
        return resultutils.results(result='get group areas map success',
                                   data=[dict(entity=k, areas=v) for k, v in maps])


@singleton.singleton
class AppEntityReuest(BaseContorller):

    CREATEAPPENTITY = {'type': 'object',
                       'properties': {
                        'objfile': {'type': 'object',
                                    'required': ['version', 'subtype'],
                                    'properties': {'version': {'type': 'string'},
                                                   'subtype': {'type': 'string'}},
                                    'description': '需要下载的文件信息'},
                        'agent_id': {'type': 'integer', 'minimum': 1,
                                     'description': '程序安装的目标机器'},
                        'cross_id': {'type': 'integer', 'minimum': 1,
                                     'description': '跨服程序的实体id'},
                        'databases': {'type': 'array',
                                      'items': {'type': 'object',
                                                'required': ['type', 'database_id'],
                                                'properties': {
                                                    'subtype': {'type': 'string',
                                                                'description': '数据类型(业务日志/业务数据)'},
                                                    'database_id': {'type': 'integer', 'minimum': 1,
                                                                    'description': '目标数据库'}}}}}
                       }

    BONDDATABASE = {'type': 'array', 'minItems': 1,
                    'items': {'type': 'object',
                              'required': ['quote_id', 'entity', 'type', 'host', 'port', 'user', 'passwd'],
                              'properties': {'quote_id': {'type': 'integer', 'minimum': 1},
                                             'entity': {'type': 'integer', 'minimum': 1},
                                             'type':  {'type': 'string'},
                                             'host':  {'type': 'string'},
                                             'port':  {'type': 'integer', 'minimum': 1, 'maxmum': 65535},
                                             'user':  {'type': 'string'},
                                             'passwd':  {'type': 'string'},
                                             }
                              }
                    }

    def _entityinfo(self, req, entity):
        entityinfo = entity_controller.show(req=req, entity=entity,
                                            endpoint=common.NAME, body={'ports': True})['data'][0]
        ports = entityinfo['ports']
        metadata = entityinfo['metadata']
        return metadata, ports

    def _agentselect(self, req, objtype, **kwargs):
        """服务器自动选择"""
        if kwargs.get('agent_id'):
            return kwargs.get('agent_id')
        zone = kwargs.get('zone', 'all')
        includes = ['metadata.zone=%s' % zone,
                    'metadata.agent_type=application',
                    'disk>=500', 'free>=200', 'cpu>=2']
        if objtype != common.CROSSSERVER:
            # 非crossserver要求存在外网ip
            includes.append('external_ips!=None')
        weighters = [{'cputime': 5},
                     {'cpu': -1},
                     {'free': 200},
                     {'left': 500},
                     {'process': None}]
        agents = self.chioces(common.NAME, includes=includes, weighters=weighters)
        LOG.info('Auto select agent %d' % agents[0])
        return agents[0]

    def _dbselect(self, req, objtype, **kwargs):
        """数据库自动选择"""
        if kwargs.get('databases'):
            return kwargs.get('databases')
        zone = kwargs.get('zone', 'all')
        # 指定亲和性
        body = dict(affinitys=common.AFFINITYS[objtype].values(),
                    dbtype='mysql', zone=zone)
        impl = kwargs.pop('impl')
        _databases = []
        # 返回排序好的可选数据库
        chioces = database_controller.select(req, impl, body)
        for chioce in chioces:
            affinity = chioce['affinity']
            databases = chioce['databases']
            if affinity & common.AFFINITYS[objtype][common.DATADB] and databases:
                _databases.append(dict(subtype=common.DATADB,
                                       database_id=databases[0].get('database_id')))
                break
        if objtype == common.GAMESERVER:
            for chioce in chioces:
                affinity = chioce['affinity']
                databases = chioce['databases']
                if affinity & common.AFFINITYS[objtype][common.LOGDB] and databases:
                    _databases.append(dict(subtype=common.LOGDB,
                                           database_id=databases[0].get('database_id')))
                break
        return _databases

    def _validate_databases(self, objtype, databases):
        datadb = 0
        logdb = 0
        for database in databases:
            if database.get('subtype') == common.DATADB:
                datadb += 1
            elif database.get('subtype') == common.LOGDB:
                logdb += 1
            else:
                raise InvalidArgument('Database subtype %s unkonwn' % database.get('subtype'))
        if objtype == common.GAMESERVER:
            if datadb == 1 and logdb == 1:
                pass
            else:
                raise InvalidArgument('%s datadb %d, logdb %d' % (objtype, datadb, logdb))
        else:
            if datadb == 1 and logdb == 0:
                pass
            else:
                raise InvalidArgument('%s datadb %d, logdb %d' % (objtype, datadb, logdb))

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
                   AppEntity.status,
                   AppEntity.objtype]

        th = eventlet.spawn(areas_map, group_id)

        option = None
        if detail:
            columns.append(AppEntity.databases)
            option = joinedload(AppEntity.databases, innerjoin=False)

        results = resultutils.bulk_results(session,
                                           model=AppEntity,
                                           columns=columns,
                                           counter=AppEntity.entity,
                                           order=order, desc=desc,
                                           option=option,
                                           filter=and_(AppEntity.group_id == group_id,
                                                       AppEntity.objtype == objtype),
                                           page_num=page_num)
        maps = th.wait()

        if not results['data']:
            return results

        emaps = entity_controller.shows(endpoint=common.NAME,
                                        entitys=[column.get('entity') for column in results['data']])

        for column in results['data']:
            entity = column.get('entity')
            entityinfo = emaps.get(entity)
            if detail:
                databases = column.get('databases', [])
                column['databases'] = []
                for database in databases:
                    column['databases'].append(dict(quote_id=database.quote_id, subtype=database.subtype,
                                                    host=database.host, port=database.port,
                                                    ))
            column.setdefault('areas', maps.get(column.get('entity', [])))
            # column['agent_id'] = entityinfo.get('agent_id')
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

        return results

    def create(self, req, group_id, objtype, body=None):
        body = body or {}
        group_id = int(group_id)
        jsonutils.schema_validate(body, self.CREATEAPPENTITY)
        # 找cross服务, gameserver专用
        cross_id = body.pop('cross_id', None)
        # 安装文件信息
        objfile = body.pop('objfile', None)
        if objfile:
            objfile = objfile_controller.find(objtype, objfile.get('subtype'), objfile.get('version'))
        LOG.info('Try find agent and database for entity')
        # 选择实例运行服务器
        agent_id = self._agentselect(req, objtype, **body)
        # 选择实例运行数据库
        databases = self._dbselect(req, objtype, **body)
        # 校验数据库信息
        self._validate_databases(objtype, databases)
        LOG.info('Find agent and database for entity success')
        session = endpoint_session()
        query = model_query(session, Group, filter=Group.group_id == group_id)
        joins = joinedload(Group.entitys, innerjoin=False)
        joins = joins.joinedload(AppEntity.databases, innerjoin=False)
        query = query.options(joins)
        _group = query.one()
        next_area = _group.lastarea + 1
        glock = get_gamelock()
        with glock.arealock(group_id, next_area):
            typemap = {}
            for _entity in _group.entitys:
                try:
                    typemap[_entity.objtype].append(_entity)
                except KeyError:
                    typemap[_entity.objtype] = [_entity, ]
            # 前置实体
            chiefs = None
            # 来源文件
            # base = None
            # 相同类型的实例列表
            same_type_entitys = typemap.get(objtype, [])
            if objtype == common.GMSERVER:
                # GM服务不允许相同实例
                if same_type_entitys:
                    return resultutils.results('create entity fail, %s duplicate in group' % objtype,
                                               resultcode=manager_common.RESULT_ERROR)
            else:
                if objtype == common.GAMESERVER:
                    # 游戏服务器需要在同组中找到gm和cross实例
                    try:
                        gm = typemap[common.GMSERVER]
                        cross = None
                        crossservers = typemap[common.CROSSSERVER]
                    except KeyError as e:
                        return resultutils.results('create entity fail, can not find my chief: %s' % e.message,
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
                                counter.setdefault(_cross.cross_id, 0)
                            # 查询当前组内所有area
                            for _area in _group.areas:
                                if _area.entity in counted:
                                    continue
                                _area.entity.add(_area.entity)
                                counter[_area.cross_id] += 1
                            # 选取引用次数最少的cross_id
                            cross_id = sorted(zip(counter.itervalues(), counter.iterkeys()))[0][1]
                            for _cross in crossservers:
                                if cross_id == _cross.entity:
                                    cross = _cross
                                    break
                    if not cross:
                        raise InvalidArgument('cross server can not be found for %s' % objtype)

                    # 获取实体相关服务器信息(端口/ip)
                    maps = entity_controller.shows(endpoint=common.NAME, entitys=[gm.entity, cross.entity])
                    for v in six.itervalues(maps):
                        if v is None:
                            raise InvalidArgument('Get chiefs info error, not online?')
                    chiefs = dict()
                    chiefs.setdefault(common.CROSSSERVER,
                                      dict(entity=cross.entity,
                                           ports=maps.get(cross.entity).get('ports'),
                                           local_ip=maps.get(cross.entity).get('metadata').get('local_ip')
                                           ))
                    chiefs.setdefault(common.GMSERVER,
                                      dict(entity=gm.entity,
                                           ports=maps.get(gm.entity).get('ports'),
                                           local_ip=maps.get(gm.entity).get('metadata').get('local_ip')
                                           ))
            # 完整的rpc数据包
            body = dict(objtype=objtype,
                        objfile=objfile,
                        databases=databases,
                        chiefs=chiefs)

            with session.begin():
                _entity = entity_controller.create(req=req, agent_id=agent_id,
                                                   endpoint=common.NAME, body=body)['data'][0]
                # 插入实体信息
                appentity = AppEntity(entity=_entity.get('entity'),
                                      agent_id=agent_id,
                                      group_id=group_id, objtype=objtype)
                session.add(appentity)
                session.flush()
                if objtype == common.GAMESERVER:
                    # 插入area数据
                    query = model_query(session, Group, filter=Group.group_id == group_id)
                    gamearea = GameArea(area_id=_group.lastarea+1,
                                        entity=appentity.appentity,
                                        group_id=_group.group_id,
                                        cross_id=chiefs.get(common.CROSSSERVER).get('entity'))
                    session.add(gamearea)
                    session.flush()
                    # 更新 group lastarea属性
                    query.update({'lastarea': next_area})

            _result = dict(entity=_entity.get('entity'), objtype=objtype, agent_id=agent_id)
            if objtype == common.GAMESERVER:
                _result.setdefault('area_id', next_area)

            threadpool.add_thread(entity_controller.post_create_entity,
                                  _entity.get('entity'), common.NAME, objtype=objtype, group_id=group_id)

            return resultutils.results(result='create %s entity success' % objtype,
                                       data=[_result, ])

    def show(self, req, group_id, objtype, entity, body=None):
        body = body or {}
        group_id = int(group_id)
        entity = int(entity)
        detail = body.get('detail', False)
        session = endpoint_session(readonly=True)
        query = model_query(session, Group, filter=Group.group_id == group_id)
        joins = joinedload(Group.entitys, innerjoin=False)
        if detail:
            joins = joins.joinedload(AppEntity.databases, innerjoin=False)
        query = query.options(joins)
        group = query.filter(and_(AppEntity.entity == entity, AppEntity.objtype == objtype)).one()
        _entity = group.entitys[0]
        metadata, ports = self._entityinfo(req, entity)
        return resultutils.results(result='show %s areas success' % objtype,
                                   data=[dict(entity=_entity.entity,
                                              agent_id=_entity.agent_id,
                                              objtype=objtype, group_id=_entity.group_id,
                                              status=_entity.status,
                                              areas=[area.area_id for area in _entity.areas],
                                              databases=[dict(quote_id=database.quote_id,
                                                              host=database.host,
                                                              port=database.port,
                                                              # user=database.user,
                                                              # passwd=database.passwd,
                                                              subtype=database.subtype,
                                                              schema='%s_%s_%s_%d' % (common.NAME,
                                                                                      objtype,
                                                                                      database.subtype,
                                                                                      entity)
                                                              )
                                                         for database in _entity.databases],
                                              metadata=metadata, ports=ports)])

    def update(self, req, group_id, objtype, entity, body=None):
            pass

    def delete(self, req, group_id, objtype, entity, body=None):
        body = body or {}
        clean = body.get('clean', 'unquote')
        if clean not in ('delete', 'unquote'):
            raise InvalidArgument('clean option value error')
        group_id = int(group_id)
        entity = int(entity)
        session = endpoint_session()
        glock = get_gamelock()
        metadata, ports = self._entityinfo(req=req, entity=entity)
        if not metadata:
            raise InvalidArgument('Agent offline, can not delete entity')
        with glock.grouplock(group=group_id):
            with session.begin():
                query = model_query(session, AppEntity, filter=and_(AppEntity.entity == entity,
                                                                    AppEntity.objtype == objtype))
                query.options(joinedload(AppEntity.databases, innerjoin=False))
                _entity = query.one()
                if _entity.status == common.OK:
                    raise InvalidArgument('Entity is active, unactive before delete it')
                if objtype == common.GMSERVER:
                    if model_count_with_key(session, AppEntity, filter=AppEntity.group_id == group_id) > 1:
                        raise InvalidArgument('You must delete other objtype entity before delete gm')
                elif objtype == common.CROSSSERVER:
                    if model_count_with_key(session, GameArea, filter=GameArea.cross_id == _entity.entity):
                        raise InvalidArgument('Cross server are reflected')

                # esure database delete
                if clean == 'delete':
                    LOG.warning('Clean option is delete, can not rollback when fail')
                    for _database in _entity.databases:
                        schema = '%s_%s_%s_%d' % (common.NAME, objtype, _database.subtype, entity)
                        quotes = schema_controller.show(req=req, database_id=_database.database_id,
                                                        schema=schema,
                                                        body={'quotes': True})['data'][0]['quotes']
                        if set(quotes) != set([_database.quote_id]):
                            result = 'delete %s:%d fail' %  (objtype, entity)
                            reason = ': database [%d].%s quote: %s' % (_database.database_id, schema, str(quotes))
                            return resultutils.results(result=(result + reason))
                        LOG.info('Delete quotes check success for %s' % schema)
                # clean database
                rollbacks = []
                for _database in _entity.databases:
                    schema = '%s_%s_%s_%d' % (common.NAME, objtype, _database.subtype, entity)
                    if clean == 'delete':
                        LOG.warning('Delete schema %s from %d' % (schema, _database.database_id))
                        try:
                            schema_controller.delete(req=req, database_id=_database.database_id,
                                                     schema=schema, body={'unquotes': [_database.quote_id]})
                        except Exception:
                            LOG.error('Delete %s from %d fail' % (schema, _database.database_id))
                            if LOG.isEnabledFor(logging.DEBUG):
                                LOG.exception('Delete schema fail')
                    elif clean == 'unquote':
                        LOG.info('Try unquote %d' % _database.quote_id)
                        try:
                            quote = schema_controller.unquote(req=req, quote_id=_database.quote_id)['data'][0]
                            if quote.get('database_id') != _database.database_id:
                                LOG.critical('quote %d with database %d, not %d' % (_database.quote_id,
                                                                                    quote.get('database_id'),
                                                                                    _database.database_id))
                                raise RuntimeError('Data error, quote database not the same')
                            rollbacks.append(dict(database_id=_database.database_id,
                                                  quote_id=_database.quote_id, schema=schema))
                        except Exception:
                            LOG.error('Unquote %d fail, try rollback' % _database.quote_id)
                token = uuidutils.generate_uuid()
                LOG.info('Send delete command with token %s' % token)
                try:
                    entity_controller.delete(req, common.NAME, entity=entity, body=dict(token=token))
                except Exception:
                    # roll back unquote
                    def _rollback():
                        for back in rollbacks:
                            __database_id = back.get('database_id')
                            __schema = back.get('schema')
                            __quote_id = back.get('quote_id')
                            body = dict(quote_id=__quote_id, entity=entity)
                            body.setdefault(dbcommon.ENDPOINTKEY, common.NAME)
                            try:
                                schema_controller.bond(req, database_id=__database_id, schema=__schema, body=body)
                            except Exception:
                                LOG.error('rollback entity %d quote %d.%s.%d fail' %
                                          (entity, __database_id, schema, __quote_id))
                    threadpool.add_thread(_rollback)
                    raise
                query.delete()
        return resultutils.results(result='delete %s:%d success' % (objtype, entity),
                                   data=[dict(entity=entity, objtype=objtype,
                                              ports=ports, metadata=metadata)])

    def bondto(self, req, entity, body=None):
        body = body or {}
        entity = int(entity)
        databases = body.pop('databases')
        session = endpoint_session()
        with session.begin():
            for subtype, database in six.iteritems(databases):
                LOG.info('Bond entity %d to database %d' % (entity, database.get('database_id')))
                session.add(AreaDatabase(quote_id=database.get('quote_id'),
                                         database_id=database.get('database_id'),
                                         entity=entity, subtype=subtype,
                                         host=database.get('host'), port=database.get('port'),
                                         user=database.get('user'), passwd=database.get('passwd'),
                                         ro_user=database.get('ro_user'), ro_passwd=database.get('ro_passwd'))
                            )
                session.flush()
        return resultutils.results(result='bond entity %d database success' % entity)

    def entitys(self, req, body=None):
        entitys = body.get('entitys')
        if not entitys:
            return resultutils.results(result='not any app entitys found')
        session = endpoint_session(readonly=True)
        query = model_query(session, AppEntity, filter=AppEntity.entity.in_(entitys))
        return resultutils.results(result='get app entitys success',
                                   data=[dict(entity=_entity.entity,
                                              group_id=_entity.group_id,
                                              objtype=_entity.objtype) for _entity in query])

    def _async_bluck_rpc(self, action, group_id, objtype, entity, body):
        body = body or {}
        if entity == 'all':
            entitys = 'all'
        else:
            entitys = argutils.map_to_int(entity)
        asyncrequest = self.create_asyncrequest(body)
        target = targetutils.target_endpoint(common.NAME)
        session = endpoint_session(readonly=True)
        query = model_query(session, AppEntity, filter=and_(AppEntity.group_id == group_id,
                                                            AppEntity.objtype == objtype))
        agents = set()
        _entitys = set()
        for _entity in query:
            if entitys == 'all' or _entity.entity in entitys:
                if action != 'stop' and _entity.status != common.OK:
                    raise InvalidArgument('Entity %d status not ok' % _entity.entity)
                agents.add(_entity.agent_id)
                _entitys.add(_entity.entity)

        if entitys!= 'all' and len(_entitys) != len(entitys):
            raise InvalidArgument('Some entitys not found')

        rpc_ctxt = dict(agents=list(agents))
        rpc_method = '%s_entitys' % action
        rpc_args = dict(entitys=list(_entitys))
        rpc_args.update(body)
        def wapper():
            self.send_asyncrequest(asyncrequest, target,
                                   rpc_ctxt, rpc_method, rpc_args)
        threadpool.add_thread(safe_func_wrapper, wapper, LOG)
        return resultutils.results(result='%s gogamechen1 entitys spawning',
                                   data=[asyncrequest.to_dict()])

    def start(self, req, group_id, objtype, entity, body):
        return self._async_bluck_rpc('start', group_id, objtype, entity, body)

    def stop(self, req, group_id, objtype, entity, body):
        return self._async_bluck_rpc('start', group_id, objtype, entity, body)

    def status(self, req, group_id, objtype, entity, body):
        return self._async_bluck_rpc('start', group_id, objtype, entity, body)
