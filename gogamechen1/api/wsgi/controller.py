# -*- coding:utf-8 -*-
import os
import six
import time
import urllib
import eventlet
import webob.exc
from six.moves import zip
import six.moves.urllib.parse as urlparse

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

import goperation
from goperation import threadpool
from goperation.utils import safe_func_wrapper
from goperation.manager import common as manager_common
from goperation.manager.api import get_client
from goperation.manager.api import rpcfinishtime
from goperation.manager.exceptions import CacheStoneError
from goperation.manager.utils import validateutils
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

from gopcdn import common as cdncommon
from gopcdn.api.wsgi.resource import CdnResourceReuest

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
cdnresource_controller = CdnResourceReuest()

CONF = cfg.CONF

CDNRESOURCE = {}

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


def resource_map(req, resource_id):
    """cache  resource info"""
    if resource_id not in CDNRESOURCE:
        with goperation.tlock('gogamechen1-cdnresource'):
            if resource_id not in CDNRESOURCE:
                info = cdnresource_controller.show(req, resource_id, body=dict(metadata=False))['data'][0]
                # entity = info.get('entity')
                agent_id = info.get('agent_id')
                port = info.get('port')
                internal = info.get('internal')
                domains = info.get('domains')
                name = info.get('name')
                etype = info.get('etype')
                # version = info.get('version')
                prefix = urllib.pathname2url(os.path.join(etype, name))
                metadata = BaseContorller.agent_metadata(agent_id)
                host = metadata.get('loacl_ip')
                if not internal:
                    if domains:
                        host = domains[0]
                    elif metadata.get('external_ips'):
                        host = metadata.get('external_ips')[0]
                schema = 'http'
                if port == 443:
                    schema = 'https'
                if port in (80, 443):
                    httpbase = '%s://%s' % (schema, host)
                else:
                    httpbase = '%s://%s:%d' % (schema, host, port)
                httpbase = urlparse.urljoin(httpbase, prefix)
                # CDNRESOURCE.setdefault(resource_id, dict(entity=entity, internal=internal, version=version,
                #                                          host=host, port=port, prefix=prefix,
                #                                          httpbase=httpbase))
                CDNRESOURCE.setdefault(resource_id, httpbase)
    return CDNRESOURCE[resource_id]


def gopcdn_upload(req, resource_id, body, notity=None):
    if not resource_id:
        raise InvalidArgument('No gopcdn resource is designated')
    httpbase = resource_map(req, resource_id)
    timeout = body.get('timeout', 30)
    impl = body.pop('impl', 'websocket')
    auth = body.pop('auth', None)

    md5 = body.get('md5')
    crc32 = body.get('crc32')
    ext = body.get('ext')
    size = body.get('size')

    filename = body.get('filename')
    overwrite = body.get('overwrite')
    if not filename:
        if not ext:
            raise InvalidArgument('not filename and ext')
        # 生成随机文件名
        filename = '%s.%s' % (str(uuidutils.generate_uuid()).replace('-', ''), ext)
    if not ext:
        ext = os.path.split(filename)[1][1:]
    if not ext:
        raise InvalidArgument('not filename and ext')

    address = urlparse.urljoin(httpbase, filename)
    uri_result = cdnresource_controller.add_file(req, resource_id,
                                                 body=dict(impl=impl,
                                                           timeout=timeout,
                                                           auth=auth,
                                                           notity=notity,
                                                           fileinfo=dict(size=size, md5=md5,
                                                                         crc32=crc32, ext=ext,
                                                                         filename=filename,
                                                                         overwrite=overwrite)))
    uri = uri_result.get('uri')
    return uri, address


@singleton.singleton
class ObjtypeFileReuest(BaseContorller):
    CREATESCHEMA = {}

    def index(self, req, body=None):
        body = body or {}
        order = body.pop('order', None)
        desc = body.pop('desc', False)
        page_num = int(body.pop('page_num', 0))
        session = endpoint_session(readonly=True)
        columns = [ObjtypeFile.uuid,
                   ObjtypeFile.objtype,
                   ObjtypeFile.subtype,
                   ObjtypeFile.version]

        results = resultutils.bulk_results(session,
                                           model=ObjtypeFile,
                                           columns=columns,
                                           counter=ObjtypeFile.uuid,
                                           order=order, desc=desc,
                                           page_num=page_num)
        return results

    def create(self, req, body=None):
        body = body or {}

        uri = None
        uuid = uuidutils.generate_uuid()

        subtype = utils.validate_string(body.pop('subtype'))
        objtype = body.pop('objtype')
        version = body.pop('version')

        md5 = body.get('md5')
        crc32 = body.get('crc32')
        ext = body.get('ext')
        size = body.get('size')
        address = body.get('address')

        # 没有地址,通过gopcdn上传
        if not address:
            # 上传结束后通知
            notity = {'success': dict(action='/files/%s' % uuid,
                                      method='PUT',
                                      body=dict(status=manager_common.DOWNFILE_FILEOK)),
                      'fail': dict(action='/gogamechen1/objfiles/%s' % uuid,
                                   method='DELETE',
                                   body=dict(status=manager_common.DOWNFILE_MISSED))}
            uri, address = gopcdn_upload(req, CONF[common.NAME].objfile_resource, body, notity=notity)
            status = manager_common.DOWNFILE_UPLOADING
        else:
            status = manager_common.DOWNFILE_FILEOK

        session = endpoint_session()
        with session.begin():
            objtype_file = ObjtypeFile(uuid=uuid, objtype=objtype,
                                       version=version, subtype=subtype)
            session.add(objtype_file)
            session.flush()
            try:
                file_controller.create(req, body=dict(uuid=uuid,
                                                      address=address,
                                                      size=size, md5=md5, crc32=crc32,
                                                      ext=ext,
                                                      status=status))
            except DBDuplicateEntry:
                raise InvalidArgument('File info Duplicate error')

        return resultutils.results('creat file for %s success' % objtype,
                                   data=[dict(uuid=objtype_file.uuid, uri=uri)])

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
                entityinfo = dict(entity=entity.entity)
                if entity.areas:
                    entityinfo.setdefault('areas', [area.area_id for area in entity.areas])
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
        query.options(joinedload(Group.entitys, innerjoin=False))
        _group = query.one()
        if _group.entitys:
            raise InvalidArgument('Group has entitys, can not be delete')
        query.delete()
        return resultutils.results(result='delete group success',
                                   data=[dict(group_id=_group.group_id, name=_group.name)])

    def chiefs(self, req, group_id, body=None):
        body = body or {}
        group_id = int(group_id)
        session = endpoint_session(readonly=True)
        query = model_query(session, AppEntity,
                            filter=and_(AppEntity.group_id == group_id,
                                        AppEntity.objtype.in_([common.GMSERVER, common.CROSSSERVER])))
        appentitys = query.all()
        chiefs = []
        entitys = set()
        for entity in appentitys:
            entitys.add(entity.entity)
        emaps = entity_controller.shows(common.NAME, entitys=entitys, ports=True, metadata=True)
        for entity in appentitys:
            entityinfo = emaps.get(entity.entity)
            metadata = entityinfo.get('metadata')
            ports = entityinfo.get('ports')
            chiefs.append(dict(entity=entity.entity,
                               objtype=entity.objtype,
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
                       'required': ['objfile'],
                       'properties': {
                           'objfile': {'oneOf':
                                           [{'type': 'object',
                                             'required': ['version', 'subtype'],
                                             'properties': {'version': {'type': 'string'},
                                                            'subtype': {'type': 'string'}}},
                                            {'type': 'string', 'format': 'uuid'}],
                                       'description': '需要下载的文件, uuid或对应信息'},
                           'agent_id': {'type': 'integer', 'minimum': 1,
                                        'description': '程序安装的目标机器,不填自动分配'},
                           'opentime': {'type': 'integer', 'minimum': 1514736000,
                                        'description': '开服时间, gameserver专用参数'},
                           'cross_id': {'type': 'integer', 'minimum': 1,
                                        'description': '跨服程序的实体id,gameserver专用参数'},
                           'zone': {'type': 'string', 'description': '安装区域,默认zone为all'},
                           'databases': {'type': 'object', 'description': '程序使用的数据库,不填自动分配'}}
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
            {'free': 200},
            {'left': 500},
            {'process': None}]
        agents = self.chioces(common.NAME, includes=includes, weighters=weighters)
        if not agents:
            raise InvalidArgument('Auto select agent fail')
        LOG.debug('Auto select agent %d' % agents[0])
        return agents[0]

    def _dbselect(self, req, objtype, **kwargs):
        """数据库自动选择"""
        if kwargs.get('databases'):
            return kwargs.get('databases')
        zone = kwargs.get('zone', 'all')
        # 指定亲和性
        body = dict(affinitys=common.DBAFFINITYS[objtype].values(),
                    dbtype='mysql', zone=zone)
        # 默认使用本地数据库
        impl = kwargs.pop('impl', 'local')
        _databases = dict()
        # 返回排序好的可选数据库
        chioces = database_controller.select(req, impl, body)['data']
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

    def _validate_databases(self, objtype, databases):
        NEEDED = common.DBAFFINITYS[objtype].keys()
        if set(NEEDED) != set(databases.keys()):
            for subtype in NEEDED:
                if subtype not in databases:
                    raise InvalidArgument('database %s.%s not set')
            raise ValueError('Databases not match database needed info')

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

        return results

    def create(self, req, group_id, objtype, body=None):
        body = body or {}
        group_id = int(group_id)
        jsonutils.schema_validate(body, self.CREATEAPPENTITY)
        # 找cross服务, gameserver专用
        cross_id = body.pop('cross_id', None)
        # 开服时间, gameserver专用
        opentime = body.pop('opentime', None)
        if objtype == common.GAMESERVER and not opentime:
            raise InvalidArgument('%s need opentime' % objtype)
        # 安装文件信息
        objfile = body.pop('objfile')
        if not isinstance(objfile, basestring):
            try:
                objfile = objfile_controller.find(objtype, objfile.get('subtype'), objfile.get('version'))
            except NoResultFound:
                raise InvalidArgument('%s of %s with versison %s can not be found' %
                                      (objfile.get('subtype'), objtype, objfile.get('version')))
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
                # 非gm实体添加需要先找到同组的gm
                try:
                    gm = typemap[common.GMSERVER][0]
                except KeyError as e:
                    return resultutils.results('create entity fail, can not find my chief: %s' % e.message,
                                               resultcode=manager_common.RESULT_ERROR)
                if objtype == common.GAMESERVER:
                    cross = None
                    # 游戏服务器需要在同组中找到cross实例
                    try:
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
                    cross_id = cross.entity
            # 完整的rpc数据包
            body = dict(objtype=objtype,
                        objfile=objfile,
                        databases=databases,
                        chiefs=chiefs)

            with session.begin():
                body.setdefault('finishtime', rpcfinishtime()[0]+5)
                _entity = entity_controller.create(req=req, agent_id=agent_id,
                                                   endpoint=common.NAME, body=body)['data'][0]
                entity = _entity.get('entity')
                notify = _entity.get('notify')
                LOG.info('Entity controller create rpc result %s' % str(notify))
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
                    gamearea = GameArea(area_id=_group.lastarea+1,
                                        group_id=_group.group_id,
                                        entity=appentity.entity)
                    session.add(gamearea)
                    session.flush()
                    # 更新 group lastarea属性
                    query.update({'lastarea': next_area})

            _result = dict(entity=entity, objtype=objtype, agent_id=agent_id,
                           databases=notify.get('databases'))
            if objtype == common.GAMESERVER:
                _result.setdefault('area_id', next_area)

            threadpool.add_thread(entity_controller.post_create_entity,
                                  _entity.get('entity'), common.NAME, objtype=objtype,
                                  opentime=opentime,
                                  group_id=group_id, areas=[next_area, ])
            return resultutils.results(result='create %s entity success' % objtype,
                                       data=[_result, ])

    def show(self, req, group_id, objtype, entity, body=None):
        body = body or {}
        group_id = int(group_id)
        entity = int(entity)
        session = endpoint_session(readonly=True)
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        query = query.options(joinedload(AppEntity.databases, innerjoin=False))
        _entity = query.one()
        if _entity.objtype != objtype:
            raise InvalidArgument('Entity is not %s' % objtype)
        if _entity.group_id != group_id:
            raise InvalidArgument('Entity group %d not match  %d' % (_entity.group_id, group_id))
        metadata, ports = self._entityinfo(req, entity)
        return resultutils.results(result='show %s areas success' % objtype,
                                   data=[dict(entity=_entity.entity,
                                              agent_id=_entity.agent_id,
                                              objtype=objtype, group_id=_entity.group_id,
                                              opentime=_entity.opentime,
                                              status=_entity.status,
                                              areas=[area.area_id for area in _entity.areas],
                                              databases=[dict(quote_id=database.quote_id,
                                                              database_id=database.database_id,
                                                              host=database.host,
                                                              port=database.port,
                                                              ro_user=database.ro_user,
                                                              ro_passwd=database.ro_passwd,
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
        clean = body.pop('clean', 'unquote')
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
                query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
                # AppEntity.objtype == objtype,
                # AppEntity.group_id == group_id))
                query.options(joinedload(AppEntity.databases, innerjoin=False))
                _entity = query.one()
                if _entity.status == common.OK:
                    raise InvalidArgument('Entity is active, unactive before delete it')
                if _entity.objtype != objtype:
                    raise InvalidArgument('Objtype not match')
                if _entity.group_id != group_id:
                    raise InvalidArgument('Group id not match')
                if objtype == common.GMSERVER:
                    if model_count_with_key(session, AppEntity, filter=AppEntity.group_id == group_id) > 1:
                        raise InvalidArgument('You must delete other objtype entity before delete gm')
                elif objtype == common.CROSSSERVER:
                    if model_count_with_key(session, AppEntity, filter=AppEntity.cross_id == _entity.entity):
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
                            result = 'delete %s:%d fail' % (objtype, entity)
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
                except Exception as e:
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
                    raise e
                query.delete()
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
        if opentime < 0 or opentime >= int(time.time()) + 86400*15:
            raise InvalidArgument('opentime value error')
        session = endpoint_session()
        with session.begin():
            query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
            _entity = query.one()
            if _entity.objtype != objtype:
                raise InvalidArgument('Entity is not %s' % objtype)
            if _entity.group_id != group_id:
                raise InvalidArgument('Entity group %d not match  %d' % (_entity.group_id, group_id))
            entityinfo = entity_controller.show(req=req, entity=entity,
                                                endpoint=common.NAME, body={'ports': False})['data'][0]
            agent_id = entityinfo['agent_id']
            metadata = entityinfo['metadata']
            target = targetutils.target_agent_by_string(metadata.get('agent_type'),
                                                        metadata.get('host'))
            target.namespace = common.NAME
            rpc = get_client()
            finishtime, timeout = rpcfinishtime()
            with session.begin():
                rpc_ret = rpc.call(target, ctxt={'finishtime': finishtime, 'agents': [agent_id, ]},
                                   msg={'method': 'opentime_entity',
                                        'args': dict(entity=entity, opentime=opentime)},
                                   timeout=timeout)
                query.update({'opentime': opentime})
            if not rpc_ret:
                raise RpcResultError('change entity opentime result is None')
            if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
                raise RpcResultError('change entity opentime fail %s' % rpc_ret.get('result'))
        return resultutils.results(result='change entity opentime' % entity)

    def bondto(self, req, entity, body=None):
        """本地记录数据库绑定信息"""
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
                                         ro_user=database.get('ro_user'), ro_passwd=database.get('ro_passwd'),
                                         character_set=database.get('character_set')
                                         )
                            )
                session.flush()
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
                                              opentime=_entity.opentime,
                                              areas=[area.area_id for area in _entity.areas],
                                              objtype=_entity.objtype) for _entity in query])

    def _async_bluck_rpc(self, action, group_id, objtype, entity, body):
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
        return resultutils.results(result='gogamechen1 %s entitys spawning' % objtype,
                                   data=[asyncrequest.to_dict()])

    def start(self, req, group_id, objtype, entity, body=None):
        return self._async_bluck_rpc('start', group_id, objtype, entity, body)

    def stop(self, req, group_id, objtype, entity, body=None):
        return self._async_bluck_rpc('start', group_id, objtype, entity, body)

    def status(self, req, group_id, objtype, entity, body=None):
        return self._async_bluck_rpc('start', group_id, objtype, entity, body)

    def reset(self, req, group_id, objtype, entity, body=None):
        body = body or {}
        group_id = int(group_id)
        entity = int(entity)
        # 重置文件信息,为空表示不需要重置文件
        objfile = body.pop('objfile', None)
        if objfile and not isinstance(objfile, basestring):
            try:
                objfile = objfile_controller.find(objtype, objfile.get('subtype'), objfile.get('version'))
            except NoResultFound:
                raise InvalidArgument('%s of %s with versison %s can not be found' %
                                      (objfile.get('subtype'), objtype, objfile.get('version')))
        # 查询entity信息
        session = endpoint_session()
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        query.options(joinedload(AppEntity.databases, innerjoin=False))
        _entity = query.one()
        if _entity.objtype != objtype:
            raise InvalidArgument('Entity is not %s' % objtype)
        if _entity.group_id != group_id:
            raise InvalidArgument('Entity group %d not match  %d' % (_entity.group_id, group_id))
        chiefs = {}
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
        miss = []
        # 必要数据库信息
        NEEDED = common.DBAFFINITYS[objtype].keys()
        # 数据库信息不匹配,从gopdb接口反查数据库信息
        if set(NEEDED) != set(databases.keys()):
            LOG.warning('Database not match, try find schema info from gopdb')
            quotes = schema_controller.quotes(req, body=dict(entitys=[entity, ], endpoint=common.NAME))['data']
            for subtype in NEEDED:
                if subtype not in databases:
                    # 从gopdb接口查询引用信息
                    schema = '%s_%s_%s_%d' % (common.NAME, objtype, subtype, entity)
                    for quote_detail in quotes:
                        # 确认引用不是从库且结构名称相等
                        if quote_detail['qdatabase_id'] == quote_detail['database_id'] \
                                and quote_detail['schema'] == schema:
                            databases.setdefault(common.DATADB,
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
        entityinfo = entity_controller.show(req=req, entity=entity,
                                            endpoint=common.NAME,
                                            body={'ports': False})['data'][0]
        # ports = entityinfo['ports']
        agent_id = entityinfo['agent_id']
        metadata = entityinfo['metadata']
        if not metadata:
            raise InvalidArgument('Agent is off line, can not reset entity')
        with session.begin():
            if objtype == common.GAMESERVER:
                cross_id = _entity.cross_id
                if cross_id is None:
                    raise ValueError('%s.%d cross_id is None' % (objtype, entity))
                query = model_query(session, AppEntity,
                                    filter=or_(AppEntity.entity == entity,
                                               AppEntity.objtype == common.GMSERVER))
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
                                                    posts=ports,
                                                    local_ip=metadata.get('local_ip'))
                if len(chiefs) != 2:
                    raise ValueError('%s.%d chiefs error' % (objtype, entity))
            # 有数据库信息遗漏
            if miss:
                for obj in miss:
                    session.add(obj)
                    session.flush()
            target = targetutils.target_agent_by_string(metadata.get('agent_type'), metadata.get('host'))
            target.namespace = common.NAME
            rpc = get_client()
            finishtime, timeout = rpcfinishtime()
            if objfile:
                finishtime += 15
                timeout += 15
            rpc_ret = rpc.call(target, ctxt={'finishtime': finishtime, 'agents': [agent_id, ]},
                               msg={'method': 'reset_entity',
                                    'args': dict(entity=entity, objfile=objfile,
                                                 databases=databases, chiefs=chiefs)},
                               timeout=timeout)
            if not rpc_ret:
                raise RpcResultError('reset entity result is None')
            if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
                raise RpcResultError('reset entity fail %s' % rpc_ret.get('result'))
            return resultutils.results(result='reset entity %d success' % entity)

#
# @singleton.singleton
# class PackageReuest(BaseContorller):
#
#     CREATRESCHEMA = {
#         'type': 'object',
#         'required': ['entity', 'name', 'group', 'version', 'mark'],
#         'properties':
#             {
#                 'entity': {'type': 'integer',  'minimum': 1},
#                 'name': {'type': 'string'},
#                 'group': {'type': 'integer',  'minimum': 0},
#                 'version': {'type': 'string'},
#                 'mark': {'type': 'string'},
#                 'magic': {'type': 'object'},
#                 'desc': {'type': 'string'},
#                 'sources': {'type': 'array', 'minItems': 1,
#                             'items': {'type': 'object',
#                                       'required': ['address', 'ptype'],
#                                       'properties': {'desc': {'type': 'string'},
#                                                      'address': {'type': 'string'},
#                                                      'ptype': {'type': 'string'}}}}
#             }
#     }
#
#     UPDATESCHEMA = {
#         'type': 'object',
#         'required': ['version'],
#         'properties':
#             {
#                 'version': {'type': 'string'},
#                 'mark': {'type': 'string'},
#                 'magic': {'type': 'object'},
#                 'desc': {'type': 'string'},
#                 'sources': {'type': 'array', 'minItems': 1,
#                             'items': {'type': 'object',
#                                       'required': ['address', 'ptype'],
#                                       'properties': {'desc': {'type': 'string'},
#                                                      'address': {'type': 'string'},
#                                                      'ptype': {'type': 'string'}}}}
#             }
#     }
#
#     SOURCESCHEMA = {
#         'type': 'object',
#         'required': ['sources'],
#         'properties': {
#             'sources': {'type': 'array', 'minItems': 1,
#                         'items': {'type': 'object',
#                                   'required': ['ptype', 'address'],
#                                   'properties': {'ptype': {'type': 'string'},
#                                                  'address': {'type': 'string'},
#                                                  'desc': {'type': 'string'},
#                                                  }}}
#         }
#     }
#
#     def index(self, req, group_id, body=None):
#         endpoint = validateutils.validate_endpoint(endpoint)
#         if endpoint == common.CDN:
#             raise InvalidArgument('Ednpoint error for cdn resource')
#         body = body or {}
#         order = body.pop('order', None)
#         page_num = int(body.pop('page_num', 0))
#         session = endpoint_session(readonly=True)
#         joins = joinedload(Package.sources, Package.checkoutresource, innerjoin=False)
#         results = resultutils.bulk_results(session,
#                                            model=Package,
#                                            columns=[Package.package_id,
#                                                     Package.name,
#                                                     Package.group,
#                                                     Package.version,
#                                                     Package.mark,
#                                                     Package.status,
#                                                     Package.magic,
#                                                     Package.checkoutresource,
#                                                     Package.sources,
#                                                     Package.desc],
#                                            counter=Package.package_id,
#                                            order=order,
#                                            option=joins,
#                                            filter=Package.endpoint == endpoint,
#                                            page_num=page_num)
#         for column in results['data']:
#             checkoutresource = column.get('checkoutresource')
#             if endpoint != checkoutresource.endpoint:
#                 raise ValueError('')
#             column['checkoutresource'] = dict(entity=checkoutresource.entity,
#                                               etype=common.EntityTypeMap[checkoutresource.etype],
#                                               name=checkoutresource.name)
#             magic = column.get('magic')
#             column['magic'] = safe_loads(magic)
#             sources = column.get('sources', [])
#             column['sources'] = []
#             for source in sources:
#                 column['sources'].append(dict(ptype=common.PackageTypeMap[source.ptype],
#                                               address=source.address))
#         return results
#
#     def create(self, req, group_id, body=None):
#         endpoint = validateutils.validate_endpoint(endpoint)
#         if endpoint == common.CDN:
#             raise InvalidArgument('Ednpoint error for cdn resource')
#         body = body or {}
#         jsonutils.schema_validate(body, self.CREATRESCHEMA)
#         entity = body.pop('entity')
#         name = body.pop('name')
#         group = body.pop('group')
#         version = body.pop('version')
#         mark = body.pop('mark')
#         magic = body.get('magic')
#         desc = body.get('desc')
#         sources = body.get('sources', [])
#         session = endpoint_session()
#         with session.begin():
#             cdnresource = model_query(session, CdnResource, filter=CdnResource.entity == entity).one()
#             if cdnresource.endpoint != endpoint:
#                 raise InvalidArgument('Add new package fail, endpoint not the same as cdn resource')
#             package = Package(entity=entity,
#                               endpoint=endpoint,
#                               name=name,
#                               group=group,
#                               version=version,
#                               mark=mark,
#                               magic=safe_dumps(magic), desc=desc)
#             session.add(package)
#             session.flush()
#             for source in sources:
#                 ptype = common.InvertPackageTypeMap[source['ptype']]
#                 session.add(PackageSource(package_id=package.package_id,
#                                           ptype=ptype,
#                                           address=source.get('address'), desc=source.get('desc')))
#                 session.flush()
#         return resultutils.results(result='Add new package success')
#
#     def show(self, req, group_id, package_id):
#         endpoint = validateutils.validate_endpoint(endpoint)
#         if endpoint == common.CDN:
#             raise InvalidArgument('Ednpoint error for cdn resource')
#         session = endpoint_session(readonly=True)
#         joins = joinedload(Package.sources, Package.checkoutresource, innerjoin=False)
#         package = model_query(session, Package, filter=Package.package_id == package_id).options(joins).one()
#         etype = common.EntityTypeMap[package.checkoutresource.etype]
#         return resultutils.results('Show package success',
#                                    data=[dict(package_id=package.package_id,
#                                               endpoint=endpoint,
#                                               name=package.name,
#                                               group=package.group,
#                                               version=package.version,
#                                               mark=package.mark,
#                                               status=package.status,
#                                               magic=safe_loads(package.magic),
#                                               sources=[dict(ptype=source.ptype,
#                                                             address=source.address,
#                                                             desc=source.desc,
#                                                             ) for source in package.sources],
#                                               desc=package.desc,
#                                               checkoutresource=dict(entity=package.checkoutresource.entity,
#                                                                     etype=etype,
#                                                                     name=package.checkoutresource.name))])
#
#     def update(self, req, group_id, package_id, body=None):
#         endpoint = validateutils.validate_endpoint(endpoint)
#         if endpoint == common.CDN:
#             raise InvalidArgument('Ednpoint error for cdn resource')
#         body = body or {}
#         jsonutils.schema_validate(body, self.UPDATESCHEMA)
#         magic = body.get('magic')
#         sources = body.get('sources', [])
#         session = endpoint_session()
#         with session.begin():
#             package = model_query(session, Package, filter=Package.package_id == package_id).one()
#             package.version = body.pop('version')
#             if body.get('desc'):
#                 package.desc = body.get('desc')
#             if package.endpoint != endpoint:
#                 raise InvalidArgument('Update package fail, endpoint not the same')
#             if magic:
#                 package.magic = package.magic.update(magic) if package.magic else magic
#             for source in sources:
#                 ptype = common.InvertPackageTypeMap[source.get('ptype')]
#                 query = model_query(session, PackageSource,
#                                     filter=and_(PackageSource.package_id == package_id,
#                                                 PackageSource.ptype == ptype))
#                 data = {'address': source.get('address')}
#                 if source.get('desc'):
#                     data.setdefault('desc', source.get('desc'))
#                 query.update(data)
#         return resultutils.results('Update package success')
#
#     def delete(self, req, group_id, package_id):
#         endpoint = validateutils.validate_endpoint(endpoint)
#         if endpoint == common.CDN:
#             raise InvalidArgument('Ednpoint error for cdn resource')
#         session = endpoint_session()
#         with session.begin():
#             package = model_query(session, Package, filter=Package.package_id == package_id).one()
#             if package.endpoint != endpoint:
#                 raise InvalidArgument('Delete package fail, endpoint not the same')
#             if package.group:
#                 raise InvalidArgument('Pacakge can not be delete, used by group %d' % package.group)
#             session.delete(package)
#         return resultutils.results('Update package success')
#
#     def add_source(self, req, group_id, package_id, body=None):
#         endpoint = validateutils.validate_endpoint(endpoint)
#         if endpoint == common.CDN:
#             raise InvalidArgument('Ednpoint error for cdn resource')
#         body = body or {}
#         jsonutils.schema_validate(body, self.SOURCESCHEMA)
#         session = endpoint_session()
#         with session.begin():
#             package = model_query(session, Package,
#                                   filter=Package.package_id == package_id).one()
#             if package.endpoint != endpoint:
#                 raise InvalidArgument('Delete package fail, endpoint not the same')
#             for source in body.get('sources'):
#                 ptype = common.PackageTypeMap[source.get('ptype')]
#                 session.add(PackageSource(package_id=package_id,
#                                           ptype=ptype,
#                                           address=source.get('address'),
#                                           desc=source.get('desc')))
#                 session.flush()
#         return resultutils.results('Update package success')
#
#     def delete_source(self, req, group_id, package_id, body=None):
#         endpoint = validateutils.validate_endpoint(endpoint)
#         if endpoint == common.CDN:
#             raise InvalidArgument('Ednpoint error for cdn resource')
#         body = body or {}
#         try:
#             ptype = common.EntityTypeMap[body.get('ptype')]
#         except KeyError:
#             raise InvalidArgument('Delete cdn package source fail, ptype error')
#         session = endpoint_session()
#         joins = joinedload(Package.sources)
#         with session.begin():
#             package = model_query(session, Package,
#                                   filter=Package.package_id == package_id).options(joins).one()
#             if package.endpoint != endpoint:
#                 raise InvalidArgument('Delete package source fail, endpoint not the same')
#             for source in package.sources:
#                 if source.ptype == ptype:
#                     session.delete(source)
#                     return resultutils.results('Delete package source success')
#         return resultutils.results('No package source match')
#
#     def update_source(self, req, group_id, package_id, body=None):
#         endpoint = validateutils.validate_endpoint(endpoint)
#         if endpoint == common.CDN:
#             raise InvalidArgument('Ednpoint error for cdn resource')
#         body = body or {}
#         try:
#             address = body.pop('address')
#             ptype = common.EntityTypeMap[body.get('ptype')]
#         except KeyError:
#             raise InvalidArgument('Update cdn package source fail, ptype or address error')
#         session = endpoint_session()
#         joins = joinedload(Package.sources)
#         with session.begin():
#             package = model_query(session, Package,
#                                   filter=Package.package_id == package_id).options(joins).one()
#             if package.endpoint != endpoint:
#                 raise InvalidArgument('Update package source fail, endpoint not the same')
#             for source in package.sources:
#                 if source.ptype == ptype:
#                     source.address = address
#                     if body.get('desc'):
#                         source.desc = body.get('desc')
#                     return resultutils.results('Update package source success')
#         return resultutils.results('No package source match')
#
#     def cdngroup(self, req, group_id, package_id, body=None):
#         endpoint = validateutils.validate_endpoint(endpoint)
#         if endpoint == common.CDN:
#             raise InvalidArgument('Ednpoint error for cdn resource')
#         body = body or {}
#         try:
#             group = body.pop('group')
#         except KeyError:
#             raise InvalidArgument('Update cdn package group fail, no group found')
#         session = endpoint_session()
#         with session.begin():
#             package = model_query(session, Package,
#                                   filter=Package.package_id == package_id).one()
#             if package.endpoint != endpoint:
#                 raise InvalidArgument('Update package source fail, endpoint not the same')
#             package.group = group
#             return resultutils.results('Update package group success')
#         return resultutils.results('No package source match')
