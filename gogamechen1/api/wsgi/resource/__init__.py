# -*- coding:utf-8 -*-
import time
import os
import urllib
import eventlet
import webob.exc
import six.moves.urllib.parse as urlparse

from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import jsonutils
from simpleutil.utils import singleton
from simpleutil.utils import digestutils
from simpleutil.config import cfg

from simpleservice.ormdb.api import model_query
from simpleservice.ormdb.api import model_count_with_key
from simpleservice.ormdb.exceptions import DBDuplicateEntry
from simpleservice.rpc.exceptions import AMQPDestinationNotFound
from simpleservice.rpc.exceptions import MessagingTimeout
from simpleservice.rpc.exceptions import NoSuchMethod

import goperation
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

from gopcdn import common as cdncommon
from gopcdn.api.wsgi.resource import CdnResourceReuest
from gopcdn.api.wsgi.resource import CdnQuoteRequest

from gogamechen1 import common
from gogamechen1 import utils

from gogamechen1.api import endpoint_session
from gogamechen1.models import AppEntity
from gogamechen1.models import ObjtypeFile
from gogamechen1.models import Package
from gogamechen1.models import PackageFile
from gogamechen1.models import PackageEntity
from gogamechen1.models import PackageRemark
from gogamechen1.api.wsgi.game import GroupReuest
from gogamechen1.api.wsgi.caches import resource_cache_map
from gogamechen1.api.wsgi.caches import map_resources

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
cdnresource_controller = CdnResourceReuest()
cdnquote_controller = CdnQuoteRequest()

group_controller = GroupReuest()

CONF = cfg.CONF

DEFAULTVALUE = object()

def resource_url(resource_id, fileinfo=None):
    resource = resource_cache_map(resource_id, flush=False)
    etype = resource.get('etype')
    name = resource.get('name')
    paths = [etype, name]
    if fileinfo:
        paths.append(fileinfo.get('filename'))
    path = urllib.pathname2url(os.path.join(*paths))
    return [urlparse.urljoin(netloc, path) for netloc in resource.get('netlocs')]


def gopcdn_upload(req, resource_id, body, fileinfo, notify=None):
    if not resource_id:
        raise InvalidArgument('No gopcdn resource is designated')
    timeout = body.get('timeout') or 30
    impl = body.pop('impl', 'websocket')
    auth = body.pop('auth', None)
    uri_result = cdnresource_controller.add_file(req, resource_id,
                                                 body=dict(impl=impl,
                                                           timeout=timeout,
                                                           auth=auth,
                                                           notify=notify,
                                                           fileinfo=fileinfo))
    uri = uri_result.get('uri')
    return uri


@singleton.singleton
class ObjtypeFileReuest(BaseContorller):

    CREATESCHEMA = {
        'type': 'object',
        'required': ['subtype', 'objtype', 'version'],
        'properties':
            {
                'subtype': {'type': 'string'},
                'objtype': {'type': 'string'},
                'version': {'type': 'string'},
                'impl': {'oneOf': [{'type': 'string'}, {'type': 'null'}],
                         'description': '上传方式,默认为websocket'},
                'auth': {'oneOf': [{'type': 'string'}, {'type': 'object'}, {'type': 'null'}],
                         'description': '上传认证相关信息'},
                'timeout': {'oneOf': [{'type': 'integer', 'minimum': 30}, {'type': 'null'}],
                            'description': '上传超时时间'},
                'address': {'oneOf': [{'type': 'string'}, {'type': 'null'}]},
                'fileinfo': cdncommon.FILEINFOSCHEMA,
            }
    }

    def index(self, req, body=None):
        body = body or {}
        order = body.pop('order', None)
        desc = body.pop('desc', False)
        page_num = int(body.pop('page_num', 0))

        filters = []
        objtype = body.get('objtype')
        subtype = body.get('subtype')
        if objtype:
            filters.append(ObjtypeFile.objtype == objtype)
        if subtype:
            filters.append(ObjtypeFile.subtype == subtype)

        session = endpoint_session(readonly=True)
        columns = [ObjtypeFile.md5,
                   ObjtypeFile.resource_id,
                   ObjtypeFile.objtype,
                   ObjtypeFile.subtype,
                   ObjtypeFile.version]

        results = resultutils.bulk_results(session,
                                           model=ObjtypeFile,
                                           columns=columns,
                                           counter=ObjtypeFile.md5,
                                           order=order, desc=desc,
                                           filter=and_(*filters) if filters else None,
                                           page_num=page_num)
        return results

    def create(self, req, body=None):
        body = body or {}
        jsonutils.schema_validate(body, self.CREATESCHEMA)
        uri = None
        subtype = utils.validate_string(body.pop('subtype'))
        objtype = body.pop('objtype')
        version = body.pop('version')

        address = body.get('address')
        fileinfo = body.pop('fileinfo', None)
        if not fileinfo:
            raise InvalidArgument('Fileinfo and address is none')

        md5 = fileinfo.get('md5')
        ext = fileinfo.get('ext') or os.path.splitext(fileinfo.get('filename'))[1][1:]
        size = fileinfo.get('size')
        if ext.startswith('.'):
            ext = ext[1:]

        objfile = ObjtypeFile(md5=md5, objtype=objtype, version=version, subtype=subtype)
        # 没有地址,通过gopcdn上传并存放
        if not address:
            resource_id = CONF[common.NAME].objfile_resource
            if not resource_id:
                raise InvalidArgument('Both address and resource_id is None')
            resource = resource_cache_map(resource_id)
            if not resource.get('internal', False):
                raise InvalidArgument('objtype file resource not a internal resource')
            address = resource_url(resource_id, fileinfo)[0]
            objfile.resource_id = resource_id
            # 上传结束后通知
            notify = {'success': dict(action='/files/%s' % md5,
                                      method='PUT',
                                      body=dict(status=manager_common.DOWNFILE_FILEOK)),
                      'fail': dict(action='/gogamechen1/objfiles/%s' % md5,
                                   method='DELETE')}
            uri = gopcdn_upload(req, resource_id, body,
                                fileinfo=fileinfo, notify=notify)
            status = manager_common.DOWNFILE_UPLOADING
        else:
            status = manager_common.DOWNFILE_FILEOK

        session = endpoint_session()
        with session.begin():
            try:
                session.add(objfile)
                session.flush()
            except DBDuplicateEntry:
                raise InvalidArgument('Objtype file version duplicate')
            try:
                file_controller.create(req, body=dict(md5=md5,
                                                      address=address,
                                                      size=size,
                                                      ext=ext,
                                                      status=status))
            except DBDuplicateEntry:
                raise InvalidArgument('File info Duplicate error')
        return resultutils.results(result='creat file for %s success' % objtype,
                                   data=[dict(md5=objfile.md5, uri=uri)])

    def show(self, req, md5, body=None):
        body = body or {}
        session = endpoint_session(readonly=True)
        query = model_query(session, ObjtypeFile, filter=ObjtypeFile.md5 == md5)
        objfile = query.one()
        show_result = file_controller.show(req, objfile.md5)
        if not show_result['data']:
            return resultutils.results(result='get file of %s fail, %s' % (md5, show_result.get('result')),
                                       resultcode=manager_common.RESULT_ERROR)
        file_info = show_result['data'][0]
        file_info.setdefault('subtype', objfile.subtype)
        file_info.setdefault('objtype', objfile.objtype)
        file_info.setdefault('version', objfile.version)
        file_info.setdefault('resource_id', objfile.resource_id)
        return resultutils.results(result='get file of %s success' % md5,
                                   data=[file_info, ])

    def delete(self, req, md5, body=None):
        body = body or {}
        session = endpoint_session(readonly=True)
        query = model_query(session, ObjtypeFile, filter=ObjtypeFile.md5 == md5)
        objfile = query.one()
        resource_id = objfile.resource_id
        with session.begin():
            session.delete(objfile)
            session.flush()
            if resource_id:
                show_result = file_controller.show(req, objfile.md5)
                if show_result['resultcode'] == manager_common.RESULT_SUCCESS:
                    file_info = show_result['data'][0]
                    rpath = urlparse.urlparse(file_info['address']).path
                    filename = os.path.basename(rpath)
                    try:
                        cdnresource_controller.delete_file(req, resource_id,
                                                           body=dict(filename=filename))
                    except NoResultFound:
                        LOG.error('Delete file from resource fail, resource disappeard')
                else:
                    LOG.error('objfile %s can not be found from file controller')
        return file_controller.delete(req, objfile.md5)

    def update(self, req, md5, body=None):
        raise NotImplementedError

    def find(self, objtype, subtype, version):
        session = endpoint_session(readonly=True)
        query = model_query(session, ObjtypeFile, filter=and_(ObjtypeFile.objtype == objtype,
                                                              ObjtypeFile.subtype == subtype,
                                                              ObjtypeFile.version == version))
        objfile = query.one()
        return objfile['md5']

    def send(self, req, md5, body=None):
        """call by client, and asyncrequest
        send file to agents
        """
        body = body or {}
        objtype = body.get('objtype')
        if body.pop('all', True):
            # 发文件到所有匹配的agent
            includes = ['metadata.agent_type=application', ]
            if objtype is not None:
                includes.insert(0, 'metadata.gogamechen1-aff&%d' % common.APPAFFINITYS[objtype])
            zone = body.get('zone', None)
            if zone:
                includes.insert(0, 'metadata.zone=%s' % zone)
            agents = self.chioces(endpoint=common.NAME, includes=includes)
            if not agents:
                return resultutils.results(result='No agents has been selected', resultcode=1)
        else:
            # 只发文件到已经有实体的agent
            session = endpoint_session(readonly=True)
            query = model_query(session, AppEntity.agent_id)
            if objtype:
                query = query.filter(AppEntity.objtype == objtype)
            agents = []
            for r in query:
                agents.append(r[0])
        agents = list(set(agents))
        asyncrequest = self.create_asyncrequest(body)
        target = targetutils.target_endpoint(common.NAME)
        target.namespace = manager_common.NAME
        rpc_method = 'getfile'
        rpc_args = {'md5': md5, 'timeout': asyncrequest.deadline - 1}
        rpc_ctxt = {}
        rpc_ctxt.setdefault('agents', agents)

        def wapper():
            self.send_asyncrequest(asyncrequest, target,
                                   rpc_ctxt, rpc_method, rpc_args)

        goperation.threadpool.add_thread(safe_func_wrapper, wapper, LOG)
        return resultutils.results(result='Send file to %s agents thread spawning' % common.NAME,
                                   data=[asyncrequest.to_dict()])


@singleton.singleton
class PackageReuest(BaseContorller):

    CREATESCHEMA = {
        'type': 'object',
        'required': ['resource_id', 'package_name', 'mark', 'platform'],
        'properties':
            {
                'resource_id': {'type': 'integer', 'minimum': 1,
                                'description': '安装包关联的游戏cdn资源'},
                'package_name': {'type': 'string'},
                'mark': {'type': 'string', 'description': '渠道标记名'},
                'platform': {'type': 'string', 'description': '平台类型'},
                'magic': {'oneOf': [{'type': 'object'},
                                    {'type': 'null'}]},
                'extension': {'oneOf': [{'type': 'object'},
                                        {'type': 'null'}]},
                'entitys': {'type': 'array',
                            'items': {'type': 'integer', 'minimum': 1},
                            'description': '包含的实体列表'},
                'desc': {'oneOf': [{'type': 'string'}, {'type': 'null'}]},
            }
    }

    UPDATESCHEMA = {
        'type': 'object',
        'properties':
            {
                'magic': {'type': 'object'},
                'extension': {'type': 'object'},
                'desc': {'type': 'string'},
                'gversion': {'oneOf': [{'type': 'integer'}, {'type': 'null'}]},
                'rversion': {'oneOf': [{'type': 'string'}, {'type': 'null'}]},
                'status': {'type': 'integer', 'enum': [common.ENABLE, common.DISABLE]}
            }
    }

    def packages(self, req, body=None):
        session = endpoint_session(readonly=True)
        query = model_query(session, Package, filter=Package.status == common.ENABLE)
        query = query.options(joinedload(Package.files, innerjoin=False))
        packages = query.all()
        resource_ids = set()
        group_ids = set()
        for package in packages:
            resource_ids.add(package.resource_id)
            group_ids.add(package.group_id)
        # 异步更新resources缓存
        th = eventlet.spawn(map_resources, resource_ids=resource_ids)
        chiefs = group_controller._chiefs(list(group_ids), cross=False)
        chiefs_maps = {}
        for chief in chiefs:
            chiefs_maps.setdefault(chief.get('group_id'), chief)
        th.wait()
        data = []
        for package in packages:
            chief = chiefs_maps[package.group_id]
            resource = resource_cache_map(resource_id=package.resource_id, flush=False)
            info = dict(dict(package_id=package.package_id,
                             package_name=package.package_name,
                             gversion=package.gversion,
                             rversion=package.rversion,
                             group_id=package.group_id,
                             etype=resource.get('etype'),
                             name=resource.get('name'),
                             mark=package.mark,
                             platform=package.platform,
                             status=package.status,
                             magic=jsonutils.loads_as_bytes(package.magic) if package.magic else None,
                             extension=jsonutils.loads_as_bytes(package.extension) if package.extension else None,
                             resource=dict(versions=resource.get('versions'),
                                           urls=resource_url(package.resource_id),
                                           resource_id=package.resource_id),
                             login=dict(local_ip=chief.get('local_ip'),
                                        ports=chief.get('ports'),
                                        objtype=chief.get('objtype'),
                                        dnsnames=chief.get('dnsnames'),
                                        external_ips=chief.get('external_ips')),
                             files=[dict(pfile_id=pfile.pfile_id,
                                         ftype=pfile.ftype,
                                         address=pfile.address,
                                         gversion=pfile.gversion,
                                         uptime=pfile.uptime,
                                         status=pfile.status)
                                    for pfile in package.files
                                    if pfile.status == manager_common.DOWNFILE_FILEOK])
                        )
            data.append(info)
        return resultutils.results(result='list packages success', data=data)

    def resources(self, req, body=None):
        session = endpoint_session(readonly=True)
        query = model_query(session, Package)
        packages = query.all()
        _resources = set()
        maps = {}
        for package in packages:
            try:
                maps[package.resource_id].append(package)
            except KeyError:
                maps[package.resource_id] = [package, ]
            _resources.add(package.resource_id)
        map_resources(_resources)

        data = []
        for resource_id in _resources:
            resource = resource_cache_map(resource_id=resource_id, flush=False)
            data.append(dict(resource_id=resource_id,
                             packages=[p.package_id for p in maps[resource_id]],
                             groups=[p.group_id for p in maps[resource_id]],
                             etype=resource.get('etype'),
                             name=resource.get('name'),
                             versions=resource.get('versions')))

        return resultutils.results(result='list packages resource success', data=data)

    def entitys(self, req, body=None):
        session = endpoint_session(readonly=True)
        query = model_query(session, Package, filter=Package.status == common.ENABLE)
        query = query.options(joinedload(Package.entitys, innerjoin=False))
        data = [dict(package_id=package.package_id,
                     package_name=package.package_name,
                     mark=package.mark,
                     entitys=[dict(entity=pentity.entity, status=pentity.status)
                              for pentity in package.entitys]
                     ) for package in query]
        return resultutils.results(result='list packages entitys success', data=data)

    def index(self, req, group_id, body=None):
        body = body or {}
        group_id = int(group_id)
        order = body.pop('order', None)
        page_num = int(body.pop('page_num', 0))
        session = endpoint_session(readonly=True)
        results = resultutils.bulk_results(session,
                                           model=Package,
                                           columns=[Package.package_id,
                                                    Package.package_name,
                                                    Package.gversion,
                                                    Package.group_id,
                                                    Package.rversion,
                                                    Package.resource_id,
                                                    Package.mark,
                                                    Package.platform,
                                                    Package.status,
                                                    Package.magic,
                                                    Package.extension],
                                           counter=Package.package_id,
                                           order=order,
                                           filter=Package.group_id == group_id,
                                           page_num=page_num)
        for column in results['data']:
            magic = column.get('magic')
            if magic:
                column['magic'] = jsonutils.loads_as_bytes(magic)

            extension = column.get('extension')
            if extension:
                column['extension'] = jsonutils.loads_as_bytes(extension)
        return results

    def create(self, req, group_id, body=None):
        body = body or {}
        group_id = int(group_id)
        jsonutils.schema_validate(body, self.CREATESCHEMA)
        resource_id = int(body.pop('resource_id'))
        package_name = body.pop('package_name')
        entitys = set(body.pop('entitys', []))
        platform = common.PlatformTypeMap.get(body.pop('platform'))
        if not platform:
            raise InvalidArgument('platform value error')
        group_id = int(group_id)
        mark = body.pop('mark')
        magic = body.get('magic')
        extension = body.get('extension')
        desc = body.get('desc')
        session = endpoint_session()
        with session.begin():
            if model_count_with_key(session, Package.package_id,
                                    filter=Package.package_name == package_name):
                raise InvalidArgument('Package name Duplicate')
            if entitys:
                query = model_query(session, AppEntity, filter=AppEntity.entity.in_(entitys))
                appentitys = query.all()
                if len(appentitys) != len(entitys):
                    raise InvalidArgument('Some entity can not be found')
                for appentity in appentitys:
                    if appentity.group_id != group_id \
                            or appentity.objtype != common.GAMESERVER \
                            or not appentity.platform & platform:
                        raise InvalidArgument('Entity not exist, create package fail')
            else:
                appentitys = []
            # 确认group以及gmsvr
            if not group_controller._chiefs(group_ids=[group_id], cross=False):
                return resultutils.results(result='Can not find entity of %s' % common.GMSERVER,
                                           resultcode=manager_common.RESULT_ERROR)
            # 确认cdn资源
            resource = resource_cache_map(resource_id)
            if resource.get('internal'):
                raise InvalidArgument('Resource is internal resrouce')
            # 资源引用数量增加
            cdnresource_controller.quote(req, resource_id)
            package = Package(resource_id=resource_id,
                              package_name=package_name,
                              group_id=group_id,
                              mark=mark,
                              platform=platform,
                              magic=jsonutils.dumps(magic) if magic else None,
                              extension=jsonutils.dumps(extension) if extension else None,
                              entitys=[PackageEntity(entity=appentity.entity) for appentity in appentitys],
                              desc=desc)
            session.add(package)
            session.flush()
        return resultutils.results(result='Add a new package success',
                                   data=[dict(package_id=package.package_id,
                                              group_id=package.group_id,
                                              package_name=package.package_name,
                                              status=package.status,
                                              mark=package.mark,
                                              name=resource.get('name'),
                                              etype=resource.get('etype'),
                                              resource=dict(resource_id=package.resource_id,
                                                            versions=resource.get('versions'),
                                                            urls=resource_url(package.resource_id),
                                                            )
                                              )])

    def show(self, req, group_id, package_id, body=None):
        body = body or {}
        group_id = int(group_id)
        package_id = int(package_id)
        session = endpoint_session(readonly=True)
        query = model_query(session, Package, filter=Package.package_id == package_id)
        query = query.options(joinedload(Package.files, innerjoin=False))
        package = query.one()
        if package.group_id != group_id:
            raise InvalidArgument('Group id not the same')
        # 确认cdn资源
        resource = resource_cache_map(package.resource_id)
        group = group_controller.show(req, package.group_id)['data'][0]
        return resultutils.results(result='Show package success',
                                   data=[dict(package_id=package.package_id,
                                              package_name=package.package_name,
                                              gversion=package.gversion,
                                              group=group,
                                              resource_id=package.resource_id,
                                              rversion=package.rversion,
                                              rquote_id=package.rquote_id,
                                              etype=resource.get('etype'),
                                              name=resource.get('name'),
                                              versions=resource.get('versions'),
                                              urls=resource_url(package.resource_id),
                                              mark=package.mark,
                                              platform=package.platform,
                                              status=package.status,
                                              magic=jsonutils.loads_as_bytes(package.magic)
                                              if package.magic else None,
                                              extension=jsonutils.loads_as_bytes(package.extension)
                                              if package.extension else None,
                                              desc=package.desc,
                                              files=[dict(pfile_id=pfile.pfile_id,
                                                          ftype=pfile.ftype,
                                                          address=pfile.address,
                                                          uptime=pfile.uptime,
                                                          status=pfile.status,
                                                          gversion=pfile.gversion,
                                                          desc=pfile.desc,
                                                          ) for pfile in package.files])
                                         ]
                                   )

    def update(self, req, group_id, package_id, body=None):
        body = body or {}
        group_id = int(group_id)
        package_id = int(package_id)
        jsonutils.schema_validate(body, self.UPDATESCHEMA)
        status = body.get('status')
        desc = body.get('desc')
        rversion = body.get('rversion', DEFAULTVALUE)
        gversion = body.get('gversion', DEFAULTVALUE)
        magic = body.get('magic')
        extension = body.get('extension')
        session = endpoint_session()
        query = model_query(session, Package, filter=Package.package_id == package_id)
        if (gversion is not DEFAULTVALUE) and gversion:
            query.options(joinedload(Package.files, innerjoin=False))
        with session.begin():
            package = query.one()
            if package.group_id != group_id:
                raise InvalidArgument('Group id not the same')
            if status:
                package.status = status
            if desc:
                package.desc = desc
            if magic is not None:
                if not magic:
                    package.magic = None
                else:
                    default_magic = jsonutils.loads_as_bytes(package.magic) if package.magic else {}
                    default_magic.update(magic)
                    package.magic = jsonutils.dumps(default_magic)
            if extension is not None:
                if not extension:
                    package.extension = None
                else:
                    default_extension = jsonutils.loads_as_bytes(package.extension) if package.extension else {}
                    default_extension.update(extension)
                    package.extension = jsonutils.dumps(default_extension)
            # 玩家版本号, 由安装包决定
            if gversion is not DEFAULTVALUE:
                if gversion:
                    if gversion in [pfile.pfile_id for pfile in package.files
                                    if pfile.status == manager_common.DOWNFILE_FILEOK]:
                        package.gversion = gversion
                    else:
                        raise InvalidArgument('Package version can not be found')
                else:
                    package.gversion = None
            # 游戏资源版本号, 由cdn相关版本号决定
            if rversion is not DEFAULTVALUE:
                if rversion:
                    # 没有引用过默认version,添加资源引用
                    if not package.rversion:
                        qresult = cdnresource_controller.vquote(req, resource_id=package.resource_id,
                                                                body=dict(version=rversion))
                        quote = qresult['data'][0]
                        package.rquote_id = quote.get('quote_id')
                        alias = quote.get('alias')
                    # 有引用,修改资源引用
                    else:
                        upresult = cdnquote_controller.update(req, quote_id=package.rquote_id,
                                                              body={'version': rversion})['data'][0]
                        alias = upresult.get('version').get('alias')
                    if not alias:
                        LOG.error('version alias is None, check it')
                    LOG.info('Package version %s with alias %s' % (rversion, alias))
                else:
                    if package.rversion:
                        delresult = cdnquote_controller.delete(req, quote_id=package.rquote_id)['data'][0]
                        version_id = delresult.get('version_id')
                        LOG.info('Package remove defalut version %s, version id %d' % (package.rversion, version_id))
                        package.rquote_id = None
                package.rversion = rversion
            session.flush()
        return resultutils.results(result='Update package success')

    def delete(self, req, group_id, package_id, body=None):
        body = body or {}
        group_id = int(group_id)
        package_id = int(package_id)
        session = endpoint_session()
        package_id = int(package_id)
        query = model_query(session, Package, filter=Package.package_id == package_id)
        query = query.options(joinedload(Package.files))
        with session.begin():
            package = query.one()
            if package.group_id != group_id:
                raise InvalidArgument('Group id not the same')
            if package.group_id != group_id:
                raise InvalidArgument('Group id not match')
            if package.files:
                raise InvalidArgument('Package files exist')
            checked = set()
            # 确认组别中没有特殊version引用
            for area in group_controller._areas(group_id=package.group_id):
                entity = area.get('entity')
                if entity in checked:
                    continue
                checked.add(entity)
                versions = area.get('versions')
                if versions:
                    for p in versions:
                        if int(p) == package_id:
                            version = versions[p]
                            raise InvalidArgument('Entity %d set package %d, version %s' %
                                                  (entity, package_id, version.get('version')))
            # 版本资源引用删除
            if package.rquote_id:
                cdnquote_controller.delete(req, package.rquote_id)
            # 资源引用计数器减少
            cdnresource_controller.unquote(req, package.resource_id)
            session.delete(package)
            session.flush()
        return resultutils.results(result='Delete package success')

    def upgrade(self, req, group_id, package_id, body=None):
        """更新资源版本"""
        body = body or {}
        group_id = int(group_id)
        package_id = int(package_id)
        session = endpoint_session(readonly=True)
        query = model_query(session, Package, filter=Package.package_id == package_id)
        package = query.one()
        if package.group_id != group_id:
            raise InvalidArgument('Group id not the same')
        # 设置detail中的endpoint
        detail = body.pop('detail', None) or {}
        detail.setdefault('endpoint', common.NAME)
        body.setdefault('detail', detail)
        result = cdnresource_controller.upgrade(req, resource_id=package.resource_id, body=body)
        return result

    def add_remark(self, req, group_id, package_id, body=None):
        body = body or {}
        group_id = int(group_id)
        package_id = int(package_id)
        if 'username' not in body:
            raise InvalidArgument('username not found')
        if 'message' not in body:
            raise InvalidArgument('message not found')
        session = endpoint_session()
        query = model_query(session, Package, filter=Package.package_id == package_id)
        package = query.one()
        if package.group_id != group_id:
            raise InvalidArgument('Group id not the same')
        if package.group_id != group_id:
            raise InvalidArgument('Group id not the same')

        remark = PackageRemark(package_id=package_id,
                               rtime=int(time.time()),
                               username=str(body.get('username')),
                               message=str(body.get('message')))
        session.add(remark)
        session.flush()
        return resultutils.results(result='Add remark success')

    def del_remark(self, req, group_id, package_id, body=None):
        body = body or {}
        group_id = int(group_id)
        package_id = int(package_id)
        remark_id = int(body.pop('remark_id'))
        session = endpoint_session()
        query = model_query(session, Package, filter=Package.package_id == package_id)
        package = query.one_or_none()
        if package and package.group_id != group_id:
            raise InvalidArgument('Group id not the same')
        query = model_query(session, PackageRemark, filter=and_(PackageRemark.package_id == package_id,
                                                                PackageRemark.remark_id == remark_id))
        query.delete()
        return resultutils.results(result='Delete remark success')

    def list_remarks(self, req, group_id, package_id, body=None):
        body = body or {}
        group_id = int(group_id)
        package_id = int(package_id)
        page_num = int(body.pop('page_num', 0))
        session = endpoint_session(readonly=True)
        query = model_query(session, Package, filter=Package.package_id == package_id)
        package = query.one()
        if package and package.group_id != group_id:
            raise InvalidArgument('Group id not the same')
        results = resultutils.bulk_results(session,
                                           model=PackageRemark,
                                           columns=[PackageRemark.rtime,
                                                    PackageRemark.username,
                                                    PackageRemark.message,
                                                    ],
                                           counter=PackageRemark.remark_id,
                                           filter=PackageRemark.package_id == package_id,
                                           order=PackageRemark.rtime,
                                           desc=True,
                                           page_num=page_num,
                                           limit=10,
                                           )
        return results


@singleton.singleton
class PackageFileReuest(BaseContorller):

    CREATESCHEMA = {
        'type': 'object',
        'required': ['ftype', 'gversion'],
        'properties':
            {
                'resource_id': {'oneOf': [{'type': 'integer', 'minimum': 1}, {'type': 'null'}],
                                'description': '安装包存放所引用的resource_id'},
                'ftype': {'type': 'string', 'description': '包类型,打包,小包,更新包等'},
                'gversion': {'type': 'string',
                             'minLength': 4,
                             'maxLength': 64,
                             'pattern': '^[0-9.]+?$',
                             'description': '安装包版本号'},
                'desc': {'oneOf': [{'type': 'string'},
                                   {'type': 'null'}]},
                'timeout': {'oneOf': [{'type': 'integer', 'minimum': 30}, {'type': 'null'}],
                            'description': '上传超时时间'},
                'impl': {'oneOf': [{'type': 'string'}, {'type': 'null'}],
                         'description': '上传方式,默认为websocket'},
                'auth': {'oneOf': [{'type': 'string'}, {'type': 'object'}, {'type': 'null'}],
                         'description': '上传认证相关信息'},
                'address': {'oneOf': [{'type': 'string'}, {'type': 'null'}],
                            'description': '文件地址, 这个字段为空才需要主动上传'},
                'fileinfo': {'oneOf': [cdncommon.FILEINFOSCHEMA, {'type': 'null'}],
                             'description': '通用file信息结构'},
            }
    }

    def index(self, req, package_id, body=None):
        body = body or {}
        order = body.pop('order', None)
        page_num = int(body.pop('page_num', 0))
        session = endpoint_session(readonly=True)
        results = resultutils.bulk_results(session,
                                           model=PackageFile,
                                           columns=[PackageFile.package_id,
                                                    PackageFile.pfile_id,
                                                    # PackageFile.quote_id,
                                                    PackageFile.ftype,
                                                    PackageFile.gversion,
                                                    PackageFile.address,
                                                    PackageFile.status,
                                                    PackageFile.uptime],
                                           counter=PackageFile.pfile_id,
                                           order=order,
                                           filter=PackageFile.package_id == package_id,
                                           page_num=page_num)
        return results

    def create(self, req, package_id, body=None):
        body = body or {}
        jsonutils.schema_validate(body, self.CREATESCHEMA)

        uri = None
        package_id = int(package_id)
        uptime = int(time.time())

        gversion = body.pop('gversion')
        ftype = body.pop('ftype')
        desc = body.get('desc')
        address = body.get('address')

        session = endpoint_session()
        if not model_count_with_key(session, Package.package_id,
                                    filter=Package.package_id == package_id):
            raise InvalidArgument('Package not exist')
        if address:
            if model_count_with_key(session, PackageFile, filter=PackageFile.address == address):
                raise InvalidArgument('Package file address duplicate')
            with session.begin():
                pfile = PackageFile(package_id=package_id, ftype=ftype,
                                    uptime=uptime, gversion=gversion,
                                    address=address, desc=desc)
                session.add(pfile)
                session.flush()
        else:
            resource_id = body.get('resource_id') or CONF[common.NAME].package_resource
            if not resource_id:
                raise InvalidArgument('Both address and resource_id is None')
            fileinfo = body.pop('fileinfo', None)
            if not fileinfo:
                raise InvalidArgument('Both fileinfo and address is none')
            resource = resource_cache_map(resource_id)
            if resource.get('internal'):
                raise InvalidArgument('apk resource is internal resource')
            address = resource_url(resource_id, fileinfo)[0]
            # 上传结束后通知
            with session.begin():
                pfile = PackageFile(package_id=package_id, ftype=ftype,
                                    resource_id=resource_id, filename=fileinfo.get('filename'),
                                    uptime=uptime, gversion=gversion,
                                    address=address, status=manager_common.DOWNFILE_UPLOADING,
                                    desc=desc)
                try:
                    session.add(pfile)
                    session.flush()
                except DBDuplicateEntry:
                    raise InvalidArgument('Address %s duplicate' % address)
                url = '/%s/package/%d/pfiles/%d' % (common.NAME, package_id, pfile.pfile_id)
                _notify = {'success': dict(action=url, method='PUT',
                                           body=dict(status=manager_common.DOWNFILE_FILEOK)),
                           'fail': dict(action=url, method='DELETE')}
                uri = gopcdn_upload(req, resource_id, body,
                                    fileinfo=fileinfo, notify=_notify)
        return resultutils.results(result='add package file for %d success' % package_id,
                                   data=[dict(pfile_id=pfile.pfile_id, uri=uri)])

    def show(self, req, package_id, pfile_id, body=None):
        session = endpoint_session(readonly=True)
        query = model_query(session, PackageFile, filter=PackageFile.pfile_id == pfile_id)
        pfile = query.one()
        if pfile.package_id != package_id:
            raise InvalidArgument('Package File package id not match')
        package = pfile.package
        return resultutils.results(result='Show package file success',
                                   data=[dict(package_id=pfile.package_id,
                                              pfile_id=pfile.pfile_id,
                                              # quote_id=pfile.quote_id,
                                              ftype=pfile.ftype,
                                              gversion=pfile.gversion,
                                              status=pfile.address,
                                              uptime=pfile.uptime,
                                              package_name=package.package_name,
                                              group_id=package.group_id,
                                              )])

    def update(self, req, package_id, pfile_id, body=None):
        body = body or {}
        package_id = int(package_id)
        pfile_id = int(pfile_id)
        status = body.get('status')
        session = endpoint_session()
        query = model_query(session, PackageFile, filter=PackageFile.pfile_id == pfile_id)
        pfile = query.one()
        if pfile.package_id != package_id:
            raise InvalidArgument('Package File package id not match')
        if pfile.status == manager_common.DOWNFILE_UPLOADING and status == manager_common.DOWNFILE_FILEOK:
            if pfile.resource_id:
                cdnresource_controller.quote(req, pfile.resource_id)
            else:
                LOG.error('Not resource_id found for package file')
        with session.begin():
            data = {'status': status}
            query.update(data)
        return resultutils.results(result='update package file  for %d success' % package_id,
                                   data=[dict(pfile_id=pfile_id)])

    def delete(self, req, package_id, pfile_id, body=None):
        body = body or {}
        package_id = int(package_id)
        pfile_id = int(pfile_id)
        session = endpoint_session()
        query = model_query(session, PackageFile, filter=PackageFile.pfile_id == pfile_id)
        pfile = query.one()
        if pfile.package_id != package_id:
            raise InvalidArgument('Package File package id not match')
        package = pfile.package
        if package.gversion == pfile.pfile_id:
            raise InvalidArgument('Package file with in quote')
        if pfile.resource_id:
            def wapper():
                if pfile.status == manager_common.DOWNFILE_FILEOK:
                    try:
                        cdnresource_controller.unquote(req, pfile.resource_id)
                    except Exception:
                        LOG.error('Revmove quote from %d fail' % pfile.resource_id)
                    if pfile.filename:
                        try:
                            cdnresource_controller.delete_file(req, pfile.resource_id,
                                                               body=dict(filename=pfile.filename))
                        except Exception:
                            LOG.error('Remove file %s from %d fail' % (pfile.resource_id, pfile.filename))
            eventlet.spawn_n(wapper)

        session.delete(pfile)
        session.flush()
        return resultutils.results(result='delete package file for %d success' % package_id,
                                   data=[dict(pfile_id=pfile_id)])
