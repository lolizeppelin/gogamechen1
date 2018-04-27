# -*- coding:utf-8 -*-
import six
import time
import requests
import inspect
import functools
import contextlib
import eventlet
import webob.exc
from six.moves import zip
from collections import OrderedDict

from requests.exceptions import ConnectionError
from requests.exceptions import ReadTimeout

from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_
from sqlalchemy.sql import or_

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import jsonutils
from simpleutil.utils import argutils
from simpleutil.utils import uuidutils
from simpleutil.utils import singleton
from simpleutil.config import cfg

from simpleservice.ormdb.api import model_query
from simpleservice.ormdb.api import model_count_with_key
from simpleservice.ormdb.exceptions import DBDuplicateEntry
from simpleservice.rpc.exceptions import AMQPDestinationNotFound
from simpleservice.rpc.exceptions import MessagingTimeout
from simpleservice.rpc.exceptions import NoSuchMethod

from goperation import threadpool
from goperation.utils import safe_func_wrapper
from goperation.manager import common as manager_common
from goperation.manager.api import get_client
from goperation.manager.api import rpcfinishtime
from goperation.manager.exceptions import CacheStoneError
from goperation.manager.utils import resultutils
from goperation.manager.utils import targetutils
from goperation.manager.wsgi.contorller import BaseContorller
from goperation.manager.wsgi.port.controller import PortReuest
from goperation.manager.wsgi.entity.controller import EntityReuest
from goperation.manager.wsgi.exceptions import RpcPrepareError
from goperation.manager.wsgi.exceptions import RpcResultError

from gopdb import common as dbcommon
from gopdb.api.wsgi.exceptions import GopdbError
from gopdb.api.wsgi.controller import SchemaReuest
from gopdb.api.wsgi.controller import DatabaseReuest

from gopcdn.api.wsgi.resource import CdnQuoteRequest
from gopcdn.api.wsgi.resource import CdnResourceReuest

from gogamechen1 import common
from gogamechen1 import utils
from gogamechen1.api import get_gamelock
from gogamechen1.api import endpoint_session

from gogamechen1.models import Group
from gogamechen1.models import AppEntity
from gogamechen1.models import GameArea
from gogamechen1.models import AreaDatabase
from gogamechen1.models import Package


LOG = logging.getLogger(__name__)

FAULT_MAP = {InvalidArgument: webob.exc.HTTPClientError,
             NoSuchMethod: webob.exc.HTTPNotImplemented,
             AMQPDestinationNotFound: webob.exc.HTTPServiceUnavailable,
             MessagingTimeout: webob.exc.HTTPServiceUnavailable,
             RpcResultError: webob.exc.HTTPInternalServerError,
             CacheStoneError: webob.exc.HTTPInternalServerError,
             RpcPrepareError: webob.exc.HTTPInternalServerError,
             NoResultFound: webob.exc.HTTPNotFound,
             MultipleResultsFound: webob.exc.HTTPInternalServerError,
             }

port_controller = PortReuest()
entity_controller = EntityReuest()
schema_controller = SchemaReuest()
database_controller = DatabaseReuest()
cdnquote_controller = CdnQuoteRequest()
cdnresource_controller = CdnResourceReuest()

CONF = cfg.CONF

def areas_map(group_id):
    session = endpoint_session(readonly=True)
    query = model_query(session, GameArea, filter=GameArea.group_id == group_id)
    maps = {}
    for _area in query:
        try:
            maps[_area.entity].append(dict(area_id=_area.area_id, show_id=_area.show_id, areaname=_area.areaname))
        except KeyError:
            maps[_area.entity] = [dict(area_id=_area.area_id, show_id=_area.show_id, areaname=_area.areaname), ]
    session.close()
    return maps


@singleton.singleton
class GroupReuest(BaseContorller):

    def index(self, req, body=None):
        body = body or {}
        order = body.pop('order', None)
        desc = body.pop('desc', False)
        page_num = int(body.pop('page_num', 0))

        session = endpoint_session(readonly=True)
        columns = [Group.group_id,
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
                column['areas'].append(dict(area_id=area.area_id, show_id=area.show_id, areaname=area.areaname))
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
                                              desc=_group.desc,
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
        query = query.options(joins)
        _group = query.one()
        group_info = dict(group_id=_group.group_id,
                          name=_group.name,
                          lastarea=_group.lastarea,
                          desc=_group.desc)
        if detail:
            _entitys = {}
            for entity in _group.entitys:
                objtype = entity.objtype
                entityinfo = dict(entity=entity.entity, status=entity.status)
                if objtype == common.GAMESERVER:
                    entityinfo.setdefault('areas', [dict(area_id=area.area_id,
                                                         show_id=area.show_id,
                                                         areaname=area.areaname)
                                                    for area in entity.areas])
                try:
                    _entitys[objtype].append(entityinfo)
                except KeyError:
                    _entitys[objtype] = [entityinfo, ]
            group_info.setdefault('entitys', _entitys)
        return resultutils.results(result='show group success', data=[group_info, ])

    def update(self, req, group_id, body=None):
        raise NotImplementedError

    def delete(self, req, group_id, body=None):
        body = body or {}
        group_id = int(group_id)
        session = endpoint_session()
        query = model_query(session, Group, filter=Group.group_id == group_id)
        query = query.options(joinedload(Group.entitys, innerjoin=False))
        _group = query.one()
        deleted = dict(group_id=_group.group_id, name=_group.name, lastarea=_group.lastarea)
        if _group.entitys:
            raise InvalidArgument('Group has entitys, can not be delete')
        session.delete(_group)
        session.flush()
        return resultutils.results(result='delete group success',
                                   data=[deleted])

    def maps(self, req, group_id, body=None):
        body = body or {}
        group_id = int(group_id)
        maps = areas_map(group_id)
        return resultutils.results(result='get group areas map success',
                                   data=[dict(entity=k, areas=v) for k, v in six.iteritems(maps)])

    def _chiefs(self, group_ids=None, cross=True):
        if cross:
            objtypes = [common.GMSERVER, common.CROSSSERVER]
        else:
            objtypes = [common.GMSERVER]
        filters = [AppEntity.objtype.in_(objtypes)]
        if group_ids:
            filters.append(AppEntity.group_id.in_(argutils.map_to_int(group_ids)))
        session = endpoint_session(readonly=True)
        query = model_query(session, AppEntity, filter=and_(*filters))
        appentitys = query.all()
        chiefs = []
        entitys = set()
        for entity in appentitys:
            entitys.add(entity.entity)
        emaps = entity_controller.shows(common.NAME, entitys=entitys, ports=True, metadata=True)
        for entity in appentitys:
            entityinfo = emaps.get(entity.entity)
            metadata = entityinfo.get('metadata')
            if not metadata:
                raise ValueError('Can not get agent metadata for %d' % entity.entity)
            ports = entityinfo.get('ports')
            chiefs.append(dict(entity=entity.entity,
                               objtype=entity.objtype,
                               group_id=entity.group_id,
                               ports=ports,
                               local_ip=metadata.get('local_ip'),
                               dnsnames=metadata.get('dnsnames'),
                               external_ips=metadata.get('external_ips'))
                          )
        return chiefs

    def chiefs(self, req, group_id, body=None):
        body = body or {}
        cross = body.get('cross', True)
        group_ids = None
        if group_id != 'all':
            group_ids = argutils.map_to_int(group_id)
        return resultutils.results(result='get group chiefs success',
                                   data=self._chiefs(group_ids, cross))

    def _areas(self, group_id, need_ok=False):
        session = endpoint_session(readonly=True)
        query = model_query(session, AppEntity,
                            filter=and_(AppEntity.group_id == group_id,
                                        AppEntity.objtype == common.GAMESERVER))
        query = query.options(joinedload(AppEntity.areas))
        appentitys = query.all()
        entitys = []
        for appentity in appentitys:
            entitys.append(appentity.entity)
        emaps = entity_controller.shows(common.NAME, entitys, ports=True, metadata=True)
        areas = []
        for appentity in appentitys:
            if need_ok and appentity.status != common.OK:
                continue
            for area in appentity.areas:
                info = dict(area_id=area.area_id,
                            show_id=area.show_id,
                            areaname=area.areaname,
                            entity=appentity.entity,
                            opentime=appentity.opentime,
                            status=appentity.status,
                            versions=jsonutils.loads_as_bytes(appentity.versions) if appentity.versions else None,
                            external_ips=emaps[appentity.entity]['metadata']['external_ips'],
                            dnsnames=emaps[appentity.entity]['metadata'].get('dnsnames'),
                            port=emaps[appentity.entity]['ports'][0])
                areas.append(info)
        return areas

    def areas(self, req, group_id, body=None):
        body = body or {}
        need_ok = body.get('need_ok', False)
        try:
            group_id = int(group_id)
        except (TypeError, ValueError):
            raise InvalidArgument('Group id value error')
        return resultutils.results(result='list group areas success',
                                   data=[dict(
                                       chiefs=self._chiefs([group_id], cross=body.get('cross', False)),
                                       areas=self._areas(group_id, need_ok))])

    def packages(self, req, group_id, body=None):
        body = body or {}
        group_id = int(group_id)
        session = endpoint_session(readonly=True)
        query = model_query(session, Group, filter=Group.group_id == group_id)
        query = query.options(joinedload(Group.packages, innerjoin=False))
        _group = query.one()
        return resultutils.results(result='list group packages success',
                                   data=[dict(package_id=package.package_id,
                                              package_name=package.package_name,
                                              mark=package.mark,
                                              status=package.status,
                                              resource_id=package.resource_id,
                                              ) for package in _group.packages])


@singleton.singleton
class AppEntityReuest(BaseContorller):

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
                           'databases': {'type': 'object', 'description': '程序使用的数据库,不填自动分配'}}
                       }

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

    @staticmethod
    def _validate_databases(objtype, databases):
        NEEDED = common.DBAFFINITYS[objtype].keys()
        if set(NEEDED) != set(databases.keys()):
            for subtype in NEEDED:
                if subtype not in databases:
                    LOG.info('database %s.%s not set' % (objtype, subtype))
                    return False
            raise InvalidArgument('Databases not match database needed info')
        return True

    def _entityinfo(self, req, entity):
        entityinfo = entity_controller.show(req=req, entity=entity,
                                            endpoint=common.NAME, body={'ports': True})['data'][0]
        ports = entityinfo['ports']
        metadata = entityinfo['metadata']
        return metadata, ports

    def _agent_chioces(self, req, objtype, **kwargs):
        """返回排序好的可选服务器列表"""
        if kwargs.get('agent_id'):
            return [kwargs.get('agent_id'), ]
        zone = kwargs.get('zone') or 'all'
        includes = ['metadata.zone=%s' % zone,
                    'metadata.gogamechen1-aff&%d' % common.APPAFFINITYS[objtype],
                    'metadata.agent_type=application',
                    'disk>=500', 'free>=200', 'cpu>=2']
        if objtype == common.GAMESERVER:
            # gameserver要求存在外网ip
            includes.append('metadata.external_ips!=None')
        weighters = [
            {'metadata.gogamechen1-aff': None},
            {'cputime': 5},
            {'cpu': -1},
            {'free': -200},
            {'left': -500},
            {'process': None}]
        chioces = self.chioces(common.NAME, includes=includes, weighters=weighters)
        LOG.debug('Auto select agent %d' % chioces[0])
        return chioces

    def _db_chioces(self, req, objtype, **kwargs):
        """返回排序好的可选数据库"""
        zone = kwargs.get('zone') or 'all'
        # 指定亲和性
        body = dict(affinitys=common.DBAFFINITYS[objtype].values(),
                    dbtype='mysql', zone=zone)
        # 默认使用本地数据库
        impl = kwargs.pop('impl', 'local')
        # 返回排序好的可选数据库
        chioces = database_controller.select(req, impl, body)['data']
        return chioces

    def _dbselect(self, req, objtype, **kwargs):
        """数据库自动选择"""
        _databases = kwargs.pop('databases', {})
        if _databases and self._validate_databases(objtype, _databases):
            return _databases
        chioces = self._db_chioces(req, objtype, **kwargs)
        if not chioces:
            raise InvalidArgument('Auto selete database fail')
        for subtype in common.DBAFFINITYS[objtype].keys():
            for chioce in chioces:
                affinity = chioce['affinity']
                databases = chioce['databases']
                if (affinity & common.DBAFFINITYS[objtype][subtype]) and databases:
                    _databases.setdefault(subtype, databases[0])
                    LOG.debug('Auto select %s.%s database %d' % (objtype, subtype, databases[0]))
                    break
        return _databases

    def _agentselect(self, req, objtype, **kwargs):
        chioces = self._agent_chioces(req, objtype, **kwargs)
        if not chioces:
            raise InvalidArgument('Auto select agent fail')
        return chioces[0]

    def databases(self, req, objtype, body=None):
        body = body or {}
        chioces = self._db_chioces(req, objtype, **body)
        return resultutils.results(result='get databases  chioces success',
                                   data=chioces)

    @staticmethod
    def _bondto(session, entity, databases):
        for subtype, database in six.iteritems(databases):
            LOG.info('Bond entity %d to database %d' % (entity, database.get('database_id')))
            session.add(AreaDatabase(quote_id=database.get('quote_id'),
                                     database_id=database.get('database_id'),
                                     entity=entity, subtype=subtype,
                                     host=database.get('host'), port=database.get('port'),
                                     user=database.get('user'), passwd=database.get('passwd'),
                                     ro_user=database.get('ro_user'), ro_passwd=database.get('ro_passwd'),
                                     character_set=database.get('character_set')
                                     )
                        )
            session.flush()

    def bondto(self, req, entity, body=None):
        """本地记录数据库绑定信息"""
        body = body or {}
        entity = int(entity)
        databases = body.pop('databases')
        session = endpoint_session()
        with session.begin():
            self._bondto(session, entity, databases)
        return resultutils.results(result='bond entity %d database success' % entity)

    def entitys(self, req, body=None):
        """批量查询entitys信息接口,内部接口agent启动的时调用"""
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
                                                          show_id=area.show_id,
                                                          areaname=area.areaname)
                                                     for area in _entity.areas],
                                              objtype=_entity.objtype) for _entity in query])

    def agents(self, req, objtype, body=None):
        body = body or {}
        chioces = self._agent_chioces(req, objtype, **body)
        return resultutils.results(result='get agents chioces success',
                                   data=chioces)

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
                   AppEntity.opentime,
                   AppEntity.versions,
                   AppEntity.status,
                   AppEntity.objtype]

        def _areas():
            maps = {}
            if objtype != common.GAMESERVER:
                return maps
            query = model_query(session, GameArea, filter=GameArea.group_id == group_id)
            for _area in query:
                try:
                    maps[_area.entity].append(dict(area_id=_area.area_id,
                                                   show_id=_area.show_id,
                                                   areaname=_area.areaname))
                except KeyError:
                    maps[_area.entity] = [dict(area_id=_area.area_id,
                                               show_id=_area.show_id,
                                               areaname=_area.areaname), ]
            # session.close()
            return maps

        th = eventlet.spawn(_areas)

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
            column.setdefault('areas', maps.get(entity, []))
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
        if objtype == common.GAMESERVER:
            if not areaname or not opentime:
                raise InvalidArgument('%s need opentime and areaname' % objtype)
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
        session = endpoint_session()
        query = model_query(session, Group, filter=Group.group_id == group_id)
        joins = joinedload(Group.entitys, innerjoin=False)
        joins = joins.joinedload(AppEntity.databases, innerjoin=False)
        query = query.options(joins)
        _group = query.one()
        next_show_area = _group.lastarea + 1
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
                                      opentime=opentime)
                session.add(appentity)
                session.flush()
                if objtype == common.GAMESERVER:
                    # 插入area数据
                    query = model_query(session, Group, filter=Group.group_id == group_id)
                    gamearea = GameArea(show_id=next_show_area,
                                        areaname=areaname.decode('utf-8')
                                        if isinstance(areaname, six.binary_type)
                                        else areaname,
                                        group_id=_group.group_id,
                                        entity=appentity.entity)
                    session.add(gamearea)
                    session.flush()
                    # 更新 group lastarea属性
                    query.update({'lastarea': next_show_area})
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
                areas = [dict(area_id=gamearea.area_id, show_id=gamearea.show_id, areaname=areaname)]
                _result.setdefault('areas', areas)
                _result.setdefault('cross_id', cross_id)
                _result.setdefault('opentime', opentime)

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
        format = body.get('format') or 'list'
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        query = query.options(joinedload(AppEntity.databases, innerjoin=False))
        _entity = query.one()
        if _entity.objtype != objtype:
            raise InvalidArgument('Entity is not %s' % objtype)
        if _entity.group_id != group_id:
            raise InvalidArgument('Entity group %d not match  %d' % (_entity.group_id, group_id))
        metadata, ports = self._entityinfo(req, entity)
        if format == 'list':
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
            if format == 'list':
                databases.append(dbinfo)
            else:
                databases[database.subtype] = dbinfo
        return resultutils.results(result='show %s areas success' % objtype,
                                   data=[dict(entity=_entity.entity,
                                              agent_id=_entity.agent_id,
                                              objtype=objtype, group_id=_entity.group_id,
                                              opentime=_entity.opentime,
                                              status=_entity.status,
                                              versions=jsonutils.loads_as_bytes(_entity.versions)
                                              if _entity.versions else None,
                                              areas=[dict(area_id=area.area_id,
                                                          show_id=area.show_id,
                                                          areaname=area.areaname.encode('utf-8'),
                                                          ) for area in _entity.areas],
                                              databases=databases,
                                              metadata=metadata, ports=ports)])

    def update(self, req, group_id, objtype, entity, body=None):
        body = body or {}
        group_id = int(group_id)
        entity = int(entity)
        status = body.get('status', common.OK)
        if status not in (common.UNACTIVE, common.OK, common.DELETED):
            raise InvalidArgument('Status not in 0, 1, 2')
        session = endpoint_session()
        glock = get_gamelock()
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        if objtype == common.GAMESERVER:
            query = query.options(joinedload(AppEntity.areas, innerjoin=False))
        _entity = query.one()
        if status == _entity.status:
            return resultutils.results(result='%s entity status in same' % objtype)
        if _entity.status == common.DELETED:
            return resultutils.results(resultcode=manager_common.RESULT_ERROR,
                                       result='%s entity has been deleted' % objtype)
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

    def quote_version(self, req, group_id, objtype, entity, body=None):
        """区服包引用指定资源版本"""
        body = body or {}
        if objtype != common.GAMESERVER:
            raise InvalidArgument('Version quote just for %s' % common.GAMESERVER)
        package_id = int(body.get('package_id'))
        rversion = body.get('rversion')
        group_id = int(group_id)
        entity = int(entity)
        session = endpoint_session()
        query = model_query(session, Group, filter=Group.group_id == group_id)
        query = query.options(joinedload(Group.packages, innerjoin=False))
        group = query.one()
        resource_id = None
        for package in group.packages:
            if package.package_id == package_id:
                resource_id = package.resource_id
        if not resource_id:
            raise InvalidArgument('Entity can not find package or package resource is None')
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        with session.begin():
            _entity = query.one()
            if _entity.objtype != objtype:
                raise InvalidArgument('Objtype not match')
            if _entity.group_id != group_id:
                raise InvalidArgument('Group id not match')
            versions = jsonutils.loads_as_bytes(_entity.versions) if _entity.versions else {}
            str_key = str(package_id)
            if str_key in versions:
                quote = versions.get(str_key)
                body = {'version': rversion}
                quote.update(body)
                cdnquote_controller.update(req, quote.get('quote_id'), body=body)
            else:
                qresult = cdnresource_controller.vquote(req, resource_id,
                                                        body={'version': rversion,
                                                              'desc': '%s.%d' % (common.NAME, entity)})
                quote = qresult['data'][0]
                quote = dict(version=rversion, quote_id=quote.get('quote_id'))
                versions.setdefault(str_key, quote)
            _entity.versions = jsonutils.dumps(versions)
            session.flush()
        return resultutils.results(result='set entity version quote success',
                                   data=[dict(resource_id=resource_id,
                                              version=rversion, quote_id=quote.get('quote_id'))])

    def unquote_version(self, req, group_id, objtype, entity, body=None):
        """区服包引用指定资源引用删除"""
        body = body or {}
        if objtype != common.GAMESERVER:
            raise InvalidArgument('Version unquote just for %s' % common.GAMESERVER)
        package_id = int(body.get('package_id'))
        group_id = int(group_id)
        entity = int(entity)
        session = endpoint_session()
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        quote = None
        with session.begin():
            _entity = query.one()
            if _entity.objtype != objtype:
                raise InvalidArgument('Objtype not match')
            if _entity.group_id != group_id:
                raise InvalidArgument('Group id not match')
            versions = jsonutils.loads_as_bytes(_entity.versions) if _entity.versions else {}
            str_key = str(package_id)
            if str_key in versions:
                quote = versions.pop(str_key)
                cdnquote_controller.delete(req, quote.get('quote_id'))
                _entity.versions = jsonutils.dumps(versions) if versions else None
                session.flush()
        return resultutils.results(result='%s entity version unquote success' % objtype,
                                   data=[dict(version=quote.get('version') if quote else None,
                                              quote_id=quote.get('quote_id') if quote else None)])

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
                    raise RpcResultError('reset entity fail %s' % rpc_ret.get('result'))
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
                        group = _entity.group
                        if not force:
                            if area.show_id != group.lastarea:
                                raise InvalidArgument('%d entity not the last area entity' % entity)
                            group.lastarea = group.lastarea - 1
                        session.flush()
                        session.delete(area)
                        session.flush()
                rpc.cast(target, ctxt={'agents': [_entity.agent_id, ]},
                         msg={'method': 'change_status',
                              'args': dict(entity=entity, status=common.DELETED)})
        return resultutils.results(result='mark %s entity delete success' % objtype,
                                   data=[dict(entity=entity, objtype=objtype,
                                              ports=ports, metadata=metadata)])

    def clean(self, req, group_id, objtype, entity, body=None):
        """彻底删除entity"""
        body = body or {}
        action = body.pop('clean', 'unquote')
        force = False
        ignores = body.pop('ignores', [])
        if action not in ('delete', 'unquote', 'force'):
            raise InvalidArgument('clean option value error')
        if action == 'force':
            action = 'delete'
            force = True
        group_id = int(group_id)
        entity = int(entity)
        session = endpoint_session()
        glock = get_gamelock()
        metadata, ports = self._entityinfo(req=req, entity=entity)
        if not metadata:
            raise InvalidArgument('Agent offline, can not delete entity')
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        query = query.options(joinedload(AppEntity.databases, innerjoin=False))
        _entity = query.one()
        with glock.grouplock(group=group_id):
            target = targetutils.target_agent_by_string(metadata.get('agent_type'),
                                                        metadata.get('host'))
            target.namespace = common.NAME
            rpc = get_client()
            finishtime, timeout = rpcfinishtime()
            LOG.warning('Clean entity %s.%d with action %s' % (objtype, entity, action))
            with session.begin():
                rpc_ret = rpc.call(target, ctxt={'finishtime': finishtime},
                                   msg={'method': 'stoped', 'args': dict(entity=entity)})
            if not rpc_ret:
                raise RpcResultError('check entity is stoped result is None')
            if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
                raise RpcResultError('check entity is stoped fail, running')

            with session.begin():
                if _entity.status != common.DELETED:
                    raise InvalidArgument('Entity status is not DELETED, '
                                          'mark status to DELETED before delete it')
                if _entity.objtype != objtype:
                    raise InvalidArgument('Objtype not match')
                if _entity.group_id != group_id:
                    raise InvalidArgument('Group id not match')
                # esure database delete
                if action == 'delete':
                    LOG.warning('Clean option is delete, can not rollback when fail')
                    if not force:
                        for _database in _entity.databases:
                            schema = '%s_%s_%s_%d' % (common.NAME, objtype, _database.subtype, entity)
                            schema_info = schema_controller.show(req=req, database_id=_database.database_id,
                                                                 schema=schema,
                                                                 body={'quotes': True})['data'][0]
                            quotes = {}
                            for _quote in schema_info['quotes']:
                                quotes[_quote.get('quote_id')] = _quote.get('desc')
                            if _database.quote_id not in quotes.keys():
                                # if set(quotes) != set([_database.quote_id]):
                                result = 'delete %s:%d fail' % (objtype, entity)
                                reason = ': database [%d].%s quote: %s' % (_database.database_id, schema, str(quotes))
                                return resultutils.results(result=(result + reason),
                                                           resultcode=manager_common.RESULT_ERROR)
                            quotes.pop(_database.quote_id)
                            for quote_id in quotes.keys():
                                if quotes[quote_id] in ignores:
                                    quotes.pop(quote_id, None)
                            if quotes:
                                if LOG.isEnabedFor(logging.DEBUG):
                                    LOG.debug('quotes not match for %d: %s' % (schema_info['schema_id'],
                                                                               schema))
                                    for quote_id in quotes.keys():
                                        LOG.debug('quote %d: %s exist' % (quote_id, quotes[quote_id]))
                                    LOG.debug('Can not delete schema before delete quotes')
                                return resultutils.results(result='Quotes not match',
                                                           resultcode=manager_common.RESULT_ERROR)
                            LOG.info('Databae quotes check success for %s' % schema)
                # clean database
                rollbacks = []
                for _database in _entity.databases:
                    schema = '%s_%s_%s_%d' % (common.NAME, objtype, _database.subtype, entity)
                    if action == 'delete':
                        LOG.warning('Delete schema %s from %d' % (schema, _database.database_id))
                        try:
                            schema_controller.delete(req=req, database_id=_database.database_id,
                                                     schema=schema, body={'unquotes': [_database.quote_id],
                                                                          'ignores': ignores, 'force': force})
                        except GopdbError as e:
                            LOG.error('Delete schema:%s from %d fail, %s' % (schema, _database.database_id,
                                                                             e.message))
                        except Exception:
                            LOG.exception('Delete schema:%s from %d fail' % (schema, _database.database_id))
                    elif action == 'unquote':
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
                session.delete(_entity)
                session.flush()
                try:
                    entity_controller.delete(req, common.NAME, entity=entity, body=dict(token=token))
                except Exception as e:
                    # roll back unquote
                    def _rollback():
                        for back in rollbacks:
                            __database_id = back.get('database_id')
                            __schema = back.get('schema')
                            __quote_id = back.get('quote_id')
                            rbody = dict(quote_id=__quote_id, entity=entity)
                            rbody.setdefault(dbcommon.ENDPOINTKEY, common.NAME)
                            try:
                                schema_controller.bond(req, database_id=__database_id, schema=__schema, body=rbody)
                            except Exception:
                                LOG.error('rollback entity %d quote %d.%s.%d fail' %
                                          (entity, __database_id, schema, __quote_id))

                    threadpool.add_thread(_rollback)
                    raise e
        return resultutils.results(result='delete %s:%d success' % (objtype, entity),
                                   data=[dict(entity=entity, objtype=objtype,
                                              ports=ports, metadata=metadata)])

    def opentime(self, req, group_id, objtype, entity, body=None):
        """修改开服时间接口"""
        body = body or {}
        group_id = int(group_id)
        entity = int(entity)
        if objtype != common.GAMESERVER:
            raise InvalidArgument('Api just for %s' % common.GAMESERVER)
        opentime = int(body.pop('opentime'))
        if opentime < 0 or opentime >= int(time.time()) + 86400 * 15:
            raise InvalidArgument('opentime value error')
        session = endpoint_session()
        with session.begin():
            query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
            _entity = query.one()
            if _entity.objtype != objtype:
                raise InvalidArgument('Entity is not %s' % objtype)
            if _entity.group_id != group_id:
                raise InvalidArgument('Entity group %d not match  %d' % (_entity.group_id, group_id))
            metadata, ports = self._entityinfo(req=req, entity=entity)
            target = targetutils.target_agent_by_string(metadata.get('agent_type'),
                                                        metadata.get('host'))
            target.namespace = common.NAME
            rpc = get_client()
            finishtime, timeout = rpcfinishtime()
            # with session.begin():
            rpc_ret = rpc.call(target, ctxt={'finishtime': finishtime},
                               msg={'method': 'opentime_entity',
                                    'args': dict(entity=entity, opentime=opentime)},
                               timeout=timeout)
            query.update({'opentime': opentime})
            if not rpc_ret:
                raise RpcResultError('change entity opentime result is None')
            if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
                raise RpcResultError('change entity opentime fail %s' % rpc_ret.get('result'))
        return resultutils.results(result='change entity %d opentime success' % entity)

    def areas(self, req, group_id, objtype, entity, body=None):
        """修改区服areas接口"""
        body = body or {}
        try:
            group_id = int(group_id)
            entity = int(entity)
            area_id = int(body.get('area_id'))
        except (TypeError, ValueError):
            raise InvalidArgument('group or area or entity id error')
        if objtype != common.GAMESERVER:
            raise InvalidArgument('Api just for %s' % common.GAMESERVER)
        raise InvalidArgument('Api is not OK')

    def _async_bluck_rpc(self, action, group_id, objtype, entity, body):
        caller = inspect.stack()[0][3]
        body = body or {}
        group_id = int(group_id)
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

        rpc_ctxt = dict(agents=list(agents))
        rpc_method = '%s_entitys' % action
        rpc_args = dict(entitys=list(entitys))
        rpc_args.update(body)

        def wapper():
            self.send_asyncrequest(asyncrequest, target,
                                   rpc_ctxt, rpc_method, rpc_args)

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
            session = endpoint_session(readonly=True)
            entitys = set()
            gm = None
            for _entity in model_query(session, AppEntity,
                                       filter=and_(AppEntity.objtype.in_([common.GAMESERVER, common.GMSERVER]),
                                                   AppEntity.group_id == group_id)):
                if _entity.status == common.DELETED:
                    continue
                if _entity.objtype == common.GMSERVER:
                    gm = _entity
                    continue
                entitys.add(_entity.entity)
            if not gm:
                return resultutils.results(result='No %s found or status is not acitve' % common.GMSERVER,
                                           resultcode=manager_common.RESULT_ERROR)
            if entity == 'all':
                entitys = list(entitys)
            else:
                targits = argutils.map_to_int(entity)
                if targits - entitys:
                    raise InvalidArgument('Entity not exist or not mark as deleted')
                entitys = targits
            entityinfo = entity_controller.show(req=req, entity=gm.entity,
                                                endpoint=common.NAME,
                                                body={'ports': True})['data'][0]
            message = body.pop('message', '') or ''
            delay = body.pop('delay', 10) or 10
            port = entityinfo.get('ports')[0]
            metadata = entityinfo.get('metadata')
            if not metadata:
                return resultutils.results(result='%s.%d is off line, can not stop by %s' %
                                                  (gm.objtype, gm.entity, gm.objtype),
                                           resultcode=manager_common.RESULT_ERROR)
            ipaddr = metadata.get('local_ip')
            url = 'http://%s:%d/closegameserver' % (ipaddr, port)
            jdata = jsonutils.dumps_as_bytes(OrderedDict(RealSvrIds=list(entitys),
                                                         Msg=message, DelayTime=delay))
            try:
                requests.post(url, data=jdata, timeout=5)
            except (ConnectionError, ReadTimeout) as e:
                return resultutils.results(result='Stop request catch %s error, %s is closed?' % (e.__class__.__name__,
                                                                                                  common.GMSERVER),
                                           resultcode=manager_common.RESULT_ERROR)
            body.update({'delay': delay})
        return self._async_bluck_rpc('stop', group_id, objtype, entity, body)

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
        body.pop('opentime', None)
        body.pop('chiefs', None)
        if objtype == common.GAMESERVER:
            gm = body.pop(common.GMSERVER, False)
            if gm:
                chiefs = {}
                session = endpoint_session()
                query = model_query(session, AppEntity,
                                    filter=and_(AppEntity.group_id == group_id,
                                                AppEntity.objtype == common.GMSERVER))
                gm = query.one()
                # query = model_query(session, (AppEntity.cross_id, func.count(AppEntity.cross_id)),
                #                     filter=and_(AppEntity.group_id == group_id,
                #                                 AppEntity.objtype == common.GMSERVER))
                # query.group_by(AppEntity.cross_id).order_by(func.count(AppEntity.cross_id))
                # 获取实体相关服务器信息(端口/ip)
                maps = entity_controller.shows(endpoint=common.NAME, entitys=[gm.entity])
                chiefs.setdefault(common.GMSERVER,
                                  dict(entity=gm.entity,
                                       ports=maps.get(gm.entity).get('ports'),
                                       local_ip=maps.get(gm.entity).get('metadata').get('local_ip')
                                       ))
                body.update({'chiefs': chiefs})
        return self._async_bluck_rpc('flushconfig', group_id, objtype, entity, body)

    def reset(self, req, group_id, objtype, entity, body=None):
        body = body or {}
        group_id = int(group_id)
        entity = int(entity)
        # 重置程序文件,为空表示不需要重置程序文件
        appfile = body.pop(common.APPFILE, None)
        # 重置数据库信息
        databases = body.pop('databases', False)
        # 重置主服务器信息(gameserver专用)
        chiefs = body.pop('chiefs', False)
        # 查询entity信息
        session = endpoint_session()
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        query = query.options(joinedload(AppEntity.databases, innerjoin=False))
        _entity = query.one()
        if _entity.objtype != objtype:
            raise InvalidArgument('Entity is not %s' % objtype)
        if _entity.group_id != group_id:
            raise InvalidArgument('Entity group %d not match  %d' % (_entity.group_id, group_id))
        entityinfo = entity_controller.show(req=req, entity=entity,
                                            endpoint=common.NAME,
                                            body={'ports': False})['data'][0]
        agent_id = entityinfo['agent_id']
        metadata = entityinfo['metadata']
        if not metadata:
            raise InvalidArgument('Agent is off line, can not reset entity')
        # 需要更新数据库
        if databases:
            miss = []
            databases = {}
            # 从本地查询数据库信息
            for database in _entity.databases:
                subtype = database.subtype
                schema = '%s_%s_%s_%d' % (common.NAME, objtype, subtype, entity)
                databases[subtype] = dict(host=database.host,
                                          port=database.port,
                                          user=database.user,
                                          passwd=database.passwd,
                                          schema=schema,
                                          character_set=database.character_set)

            # 必要数据库信息
            NEEDED = common.DBAFFINITYS[objtype].keys()
            # 数据库信息不匹配,从gopdb接口反查数据库信息
            if set(NEEDED) != set(databases.keys()):
                LOG.warning('Database not match, try find schema info from gopdb')
                quotes = schema_controller.quotes(req, body=dict(entitys=[entity, ],
                                                                 endpoint=common.NAME))['data']
                for subtype in NEEDED:
                    if subtype not in databases:
                        # 从gopdb接口查询引用信息
                        schema = '%s_%s_%s_%d' % (common.NAME, objtype, subtype, entity)
                        for quote_detail in quotes:
                            # 确认引用不是从库且结构名称相等
                            if quote_detail['qdatabase_id'] == quote_detail['database_id'] \
                                    and quote_detail['schema'] == schema:
                                databases.setdefault(subtype,
                                                     dict(host=quote_detail['host'],
                                                          port=quote_detail['port'],
                                                          user=quote_detail['user'],
                                                          passwd=quote_detail['passwd'],
                                                          schema=schema,
                                                          character_set=quote_detail['character_set']))
                                miss.append(AreaDatabase(quote_id=quote_detail['quote_id'],
                                                         database_id=quote_detail['qdatabase_id'],
                                                         entity=entity,
                                                         subtype=subtype,
                                                         host=quote_detail['host'], port=quote_detail['port'],
                                                         user=quote_detail['user'], passwd=quote_detail['passwd'],
                                                         ro_user=quote_detail['ro_user'],
                                                         ro_passwd=quote_detail['ro_passwd'],
                                                         character_set=quote_detail['character_set'])
                                            )
                                quotes.remove(quote_detail)
                                break
                        if subtype not in databases:
                            LOG.critical('Miss database of %s' % schema)
                            # 数据库信息无法从gopdb中反查到
                            raise ValueError('Not %s.%s database found for %d' % (objtype, subtype, entity))
            self._validate_databases(objtype, databases)
            # 有数据库信息遗漏
            if miss:
                with session.begin():
                    for obj in miss:
                        session.add(obj)
                        session.flush()

        if objtype == common.GAMESERVER and chiefs:
            chiefs = {}
            cross_id = _entity.cross_id
            if cross_id is None:
                raise ValueError('%s.%d cross_id is None' % (objtype, entity))
            query = model_query(session, AppEntity,
                                filter=and_(AppEntity.group_id == group_id,
                                            or_(AppEntity.entity == cross_id,
                                                AppEntity.objtype == common.GMSERVER)))
            _chiefs = query.all()
            if len(_chiefs) != 2:
                raise ValueError('Try find %s.%d chiefs from local database error' % (objtype, entity))
            for chief in _chiefs:
                for _objtype in (common.GMSERVER, common.CROSSSERVER):
                    _metadata, ports = self._entityinfo(req, chief.entity)
                    if not _metadata:
                        raise InvalidArgument('Metadata of %s.%d is none' % (_objtype, chief.entity))
                    if chief.objtype == _objtype:
                        chiefs[_objtype] = dict(entity=chief.entity,
                                                ports=ports,
                                                local_ip=metadata.get('local_ip'))
            if len(chiefs) != 2:
                raise ValueError('%s.%d chiefs error' % (objtype, entity))

        target = targetutils.target_agent_by_string(metadata.get('agent_type'), metadata.get('host'))
        target.namespace = common.NAME
        rpc = get_client()
        finishtime, timeout = rpcfinishtime()
        if appfile:
            finishtime += 30
            timeout += 35
        rpc_ret = rpc.call(target, ctxt={'finishtime': finishtime, 'agents': [agent_id, ]},
                           msg={'method': 'reset_entity',
                                'args': dict(entity=entity, appfile=appfile,
                                             opentime=_entity.opentime,
                                             databases=databases, chiefs=chiefs)},
                           timeout=timeout)
        if not rpc_ret:
            raise RpcResultError('reset entity result is None')
        if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
            raise RpcResultError('reset entity fail %s' % rpc_ret.get('result'))
        return resultutils.results(result='reset entity %d success' % entity)
