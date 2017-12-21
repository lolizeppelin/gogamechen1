# -*- coding:utf-8 -*-
import re
import random
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
        objtype = body.get('objtype')
        session = endpoint_session(readonly=True)
        query = model_query(session, ObjtypeFile)
        if objtype:
            query.filter_by(objtype=objtype)
        return resultutils.results('show file of %s success' % common.NAME,
                                   data=[dict(uuid=_file.uuid, objtype=_file.objtype, version=_file.version)
                                         for _file in query])

    def create(self, req, body=None):
        body = body or {}
        objtype = body.pop('objtype')
        version = body.pop('version')
        session = endpoint_session()
        objtype_file = ObjtypeFile(objtype=objtype, version=version)
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
        file_info.setdefault('objtype', objtype_file.objtype_file)
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


@singleton.singleton
class GroupReuest(BaseContorller):

    def index(self, req, body=None):
        pass

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
        details = body.get('details', False)
        session = endpoint_session(readonly=True)
        query = model_query(session, Group, filter=Group.group_id == group_id)
        if details:
            query.options(joinedload(Group.entitys).joinedload(AppEntity.areas))
        query.options(joinedload(Group.entitys))
        _group = query.one()
        group_info = dict(group_id=_group.group_id,
                          name=_group.name,
                          lastarea=_group.lastarea)
        if details:
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


@singleton.singleton
class AppEntityReuest(BaseContorller):

    def index(self, req, group_id, objtype, body=None):
        pass

    def create(self, req, group_id, objtype, body=None):
        body = body or {}
        # 文件版本
        version = body.pop('version')
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
                    version=version,
                    databases=databases,
                    chiefs=chiefs)
        with session.begin():
            _entity = entity_controller.create(req=req, agent_id=agent_id,
                                               endpoint=common.NAME, body=body)['data'][0]
            appentity = AppEntity(_entity=_entity.get('entity'),
                                  group_id=group_id, objtype=objtype)
            session.add(appentity)
            session.flush()
            if objtype == common.GAMESERVER:
                query = model_query(session, Group, filter=Group.group_id == group_id)
                area_id = _group.lastarea + 1
                gamearea = GameArea(area_id=_group.lastarea+1,
                                    entity=appentity.appentity,
                                    group_id=_group.group_id,
                                    cross_id=chiefs.get(common.CROSSSERVER))
                session.add(gamearea)
                session.flush()
                query.update({'lastarea': area_id})

        _result = dict(entity=_entity.get('entity'), objtype=objtype, agent_id=agent_id)
        if objtype == common.GAMESERVER:
            _result.setdefault('area_id', area_id)
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
        return resultutils.results(result='show %s areas success' % objtype,
                                   data=[dict(entity=_entity.entity, objtype=objtype,
                                              areas=[area.area_id for area in _entity.areas],
                                              databases=[dict(quote_id=database.quote_id,
                                                              host=database.host,
                                                              port=database.port,
                                                              user=database.user,
                                                              passwd=database.passwd,
                                                              type=database.type,
                                                              schema='%s_%s_%s_%d' % (common.NAME,
                                                                                      objtype,
                                                                                      database.type,
                                                                                      entity)
                                                              )
                                                         for database in _entity.databases])])

    def update(self, req, group_id, objtype, entity, body=None):
            pass

    def delete(self, req, group_id, objtype, entity, body=None):
            pass
