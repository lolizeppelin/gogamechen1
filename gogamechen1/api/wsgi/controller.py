# -*- coding:utf-8 -*-
import re
import random
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
from simpleutil.utils import singleton

from simpleservice.ormdb.api import model_query
from simpleservice.rpc.exceptions import AMQPDestinationNotFound
from simpleservice.rpc.exceptions import MessagingTimeout
from simpleservice.rpc.exceptions import NoSuchMethod


from goperation.manager import common as manager_common
from goperation.manager.exceptions import CacheStoneError
from goperation.manager.utils import resultutils
from goperation.manager.wsgi.contorller import BaseContorller
from goperation.manager.wsgi.entity.controller import EntityReuest
from goperation.manager.wsgi.file.controller import FileReuest
from goperation.manager.wsgi.exceptions import RpcPrepareError
from goperation.manager.wsgi.exceptions import RpcResultError

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
        subtype = body.pop('subtype', None)
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
        name = body.get('name')
        desc = body.get('desc')
        _group = Group(group=name, desc=desc)
        session.add(_group)
        session.flush()
        return resultutils.results(result='create group success',
                                   data=[dict(group_id=_group.group_id,
                                              name=_group.name,
                                              lastarea=_group.lastarea)])

    def show(self, req, group_id, body=None):
        body = body or {}
        detail = body.get('detail', False)
        session = endpoint_session(readonly=True)
        query = model_query(session, Group, filter=Group.group_id == group_id)
        joins = joinedload(Group.entitys)
        if detail:
            joins = joins.joinedload(AppEntity.areas)
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
        session = endpoint_session()
        query = model_query(session, Group, filter=Group.group_id == group_id)
        query.options(joinedload(Group.entitys))
        _group = query.one()
        if _group.entitys:
            raise
        query.delete()
        return resultutils.results(result='delete group success',
                                   data=[dict(group_id=_group.group_id, name=_group.name)])

    def maps(self, req, group_id, body=None):
        maps = areas_map(group_id)
        return resultutils.results(result='get group areas map success',
                                   data=[maps, ])

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
        attributes = entityinfo['attributes']
        return attributes, ports


    def index(self, req, group_id, objtype, body=None):
        body = body or {}
        order = body.pop('order', None)
        desc = body.pop('desc', False)
        detail = body.pop('detail', False)
        page_num = int(body.pop('page_num', 0))

        session = endpoint_session(readonly=True)
        columns = [AppEntity.entity,
                  AppEntity.group_id,
                  AppEntity.name,
                  AppEntity.objtype,
                  AppEntity.desc,
                  ]

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

        for column in results['data']:
            if detail:
                databases = column.get('databases', [])
                column['databases'] = []
                for database in databases:
                    column['databases'].append(dict(quote_id=database.quote_id, subtype=database.subtype,
                                                    host=database.host, port=database.port,
                                                    ))
            column.setdefault('areas', maps.get(column.get('entity', [])))
        return results


    def create(self, req, group_id, objtype, body=None):
        body = body or {}
        jsonutils.schema_validate(body, self.CREATEAPPENTITY)
        # 安装文件信息
        objfile = body.pop('objfile', None)
        if objfile:
            objfile = objfile_controller.find(objtype, objfile.get('subtype'), objfile.get('version'))

        def appselect(*args, **kwargs):
            return 1

        def dbselect(objtype, *args, **kwargs):
            _databases = [dict(subtype=common.DATADB, database_id=1)]
            if objtype == common.GAMESERVER:
                _databases.append(dict(subtype=common.LOGDB, database_id=1))
            return _databases

        # 选择实例运行服务器
        agent_id = appselect(objtype, body.get('agent_id'))
        # 选择实例运行数据库
        databases = dbselect(objtype, body.get('databases'))
        session = endpoint_session()
        query = model_query(session, Group, filter=Group.group_id == group_id)
        joins = joinedload(Group.entitys)
        joins = joins.joinedload(AppEntity.databases)
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
                # 基于相同类型的实例进行复制
                # base = dict()
                # if same_type_entitys:
                #     index = random.randint(0, len(same_type_entitys) - 1)
                #     base_entity = _group.entitys[index].entity
                #     for _database in base_entity.databases:
                #         base.setdefault(_database.type, _database.quote_id)
                if objtype == common.GAMESERVER:
                    # 游戏服务器需要在同组中找到gm和cross实例
                    try:
                        gm = typemap[common.GMSERVER][0].entity
                        crossservers = typemap[common.CROSSSERVER]
                    except KeyError as e:
                        return resultutils.results('create entity fail, can not find my chief: %s' % e.message,
                                                   resultcode=manager_common.RESULT_ERROR)
                    # 设置chiefs列表
                    chiefs = {common.GMSERVER: gm}
                    cross_id = body.get('cross_id')
                    # 如果指定了cross实例id
                    if cross_id:
                        # 判断cross实例id是否在当前组中
                        for _cross in crossservers:
                            if cross_id == _cross:
                                chiefs.setdefault(common.CROSSSERVER, cross_id)
                                break
                    else:
                        # 游戏服没有相同实例,直接使用第一个cross实例
                        if not same_type_entitys:
                            chiefs.setdefault(common.CROSSSERVER, crossservers[0].entity)
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
                            # 选取引用次数最少的cross实例
                            chiefs.setdefault(common.CROSSSERVER,
                                              zip(counter.itervalues(), counter.iterkeys()).sort()[0][1])
                    if chiefs.get(common.CROSSSERVER, None) is None:
                        raise InvalidArgument('cross server can not be found for %s' % objtype)

            body = dict(objtype=objtype,
                        objfile=objfile,
                        databases=databases,
                        chiefs=chiefs)
            with session.begin():
                _entity = entity_controller.create(req=req, agent_id=agent_id,
                                                   endpoint=common.NAME, body=body)['data'][0]
                appentity = AppEntity(entity=_entity.get('entity'),
                                      agent_id=_entity.get('agent_id'),
                                      group_id=group_id, objtype=objtype)
                session.add(appentity)
                session.flush()
                if objtype == common.GAMESERVER:
                    query = model_query(session, Group, filter=Group.group_id == group_id)
                    gamearea = GameArea(area_id=_group.lastarea+1,
                                        entity=appentity.appentity,
                                        group_id=_group.group_id,
                                        cross_id=chiefs.get(common.CROSSSERVER))
                    session.add(gamearea)
                    session.flush()
                    query.update({'lastarea': next_area})

            _result = dict(entity=_entity.get('entity'), objtype=objtype, agent_id=agent_id)
            if objtype == common.GAMESERVER:
                _result.setdefault('area_id', next_area)
            return resultutils.results(result='create %s entity success' % objtype,
                                       data=[_result, ])

    def show(self, req, group_id, objtype, entity, body=None):
        body = body or {}
        detail = body.get('detail', False)
        session = endpoint_session(readonly=True)
        query = model_query(session, Group, filter=Group.group_id == group_id)
        joins = joinedload(Group.entitys)
        if detail:
            joins = joins.joinedload(AppEntity.databases)
        query = query.options(joins)
        group = query.filter(and_(AppEntity.entity == entity, AppEntity.objtype == objtype)).one()
        _entity = group.entitys[0]
        attributes, ports = self._entityinfo(req, entity)
        return resultutils.results(result='show %s areas success' % objtype,
                                   data=[dict(entity=_entity.entity, objtype=objtype,
                                              status=_entity.status,
                                              areas=[area.area_id for area in _entity.areas],
                                              databases=[dict(quote_id=database.quote_id,
                                                              host=database.host,
                                                              port=database.port,
                                                              user=database.user,
                                                              passwd=database.passwd,
                                                              subtype=database.subtype,
                                                              schema='%s_%s_%s_%d' % (common.NAME,
                                                                                      objtype,
                                                                                      database.type,
                                                                                      entity)
                                                              )
                                                         for database in _entity.databases],
                                              attributes=attributes, ports=ports)])

    def update(self, req, group_id, objtype, entity, body=None):
            pass

    def delete(self, req, group_id, objtype, entity, body=None):
        body = body or {}

    def bondto(self, req, entity, body=None):
        body = body or {}
        databases = body.pop('databases')
        session = endpoint_session()
        with session.begin():
            for database in databases:
                session.add(AreaDatabase(quote_id=database.get('quote_id'), database_id=database.get('database_id'),
                                         entity=database.get('entity'), subtype=database.get('subtype'),
                                         host=database.get('host'), port=database.get('port'),
                                         user=database.get('user'), passwd=database.get('passwd'),
                                         ro_user=database.get('ro_user'), ro_passwd=database.get('ro_passwd'),
                                         )
                            )
                session.flush()
        return resultutils.results(result='bond entity %d database success' % entity)

    def chiefs(self, req, body=None):
        body = body or {}
        chiefs = body.pop('chiefs')
        detail = body.get('detail')

        _chiefs = {}
        session = endpoint_session(readonly=True)
        query = model_query(session, AppEntity, filter=AppEntity.entity.in_(chiefs.value))

        if detail:
            query = query.options(joinedload(AppEntity.databases))
            g = eventlet.spawn(entity_controller._shows, endpoint=common.NAME, entitys=chiefs.value)


        for _entity in query:
            if _entity.objtype in chiefs:
                _chiefs[_entity.objtype] = dict(entity=_entity.entity,
                                                group_id=_entity.group_id,
                                                agent_id=_entity.agent_id)
                if detail:
                    _chiefs[_entity.objtype].setdefault('databases', [dict(quote_id=_database.quote_id,
                                                                           subtype=_database.subtype,
                                                                           host=_database.host,
                                                                           port=_database.port,
                                                                           user=_database.user,
                                                                           passwd=_database.passwd)
                                                                      for _database in _entity.databases])
        if len(chiefs) != len(_chiefs):
            raise InvalidArgument('chiefs entity can not be found')
        if detail:
            entitysinfo = g.wait()
            for entity in _chiefs.values():
                _entity = entity.get('entity')
                entity.setdefault('ports', entitysinfo[_entity]['ports'])
                entity.setdefault('attributes', entitysinfo[_entity]['attributes'])

        return resultutils.results(result='show chiefs success',
                                   data=[_chiefs, ])


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