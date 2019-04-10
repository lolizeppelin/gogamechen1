# -*- coding:utf-8 -*-
import time
import eventlet

from sqlalchemy.orm import joinedload
from sqlalchemy.sql import and_
from sqlalchemy.sql import or_

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
from goperation.manager.wsgi.entity.controller import EntityReuest
from goperation.manager.wsgi.exceptions import RpcResultError

from gopdb import common as dbcommon
from gopdb.api.exceptions import GopdbError
from gopdb.api.wsgi.controller import SchemaReuest

from gopcdn.api.wsgi.resource import CdnQuoteRequest
from gopcdn.api.wsgi.resource import CdnResourceReuest

from gogamechen1 import common
from gogamechen1.api import get_gamelock
from gogamechen1.api import endpoint_session

from gogamechen1.models import Group
from gogamechen1.models import AppEntity
from gogamechen1.models import AreaDatabase
from gogamechen1.models import PackageArea

from .base import AppEntityReuestBase

LOG = logging.getLogger(__name__)

entity_controller = EntityReuest()
schema_controller = SchemaReuest()
cdnquote_controller = CdnQuoteRequest()
cdnresource_controller = CdnResourceReuest()

CONF = cfg.CONF


class AppEntitySyncReuest(AppEntityReuestBase):
    """sync ext function"""

    MIGRATE = {'type': 'object',
               'required': [common.APPFILE, 'new'],
               'properties': {
                   common.APPFILE: {'type': 'string', 'format': 'md5', 'description': '程序文件md5'},
                   'new': {'type': 'integer', 'minimum': 1,'description': '迁移的目标机器'}}
               }

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
                                if LOG.isEnabledFor(logging.DEBUG):
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
        query = query.options(joinedload(AppEntity.areas, innerjoin=False))
        with session.begin():
            _entity = query.one()
            if _entity.objtype != objtype:
                raise InvalidArgument('Objtype not match')
            if _entity.group_id != group_id:
                raise InvalidArgument('Group id not match')
            if not model_count_with_key(session, PackageArea.package_id,
                                        filter=and_(PackageArea.package_id == package_id,
                                                    PackageArea.area_id.in_([area.area_id
                                                                             for area in _entity.areas])
                                                    )):
                raise InvalidArgument('Entity area not in package areas')
            versions = jsonutils.loads_as_bytes(_entity.versions) if _entity.versions else {}
            str_key = str(package_id)
            if str_key in versions:
                quote = versions.get(str_key)
                if quote.get('version') != rversion:
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

    def reset(self, req, group_id, objtype, entity, body=None):
        """重置Entity程序以及配置"""
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
                                                local_ip=_metadata.get('local_ip'))
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

    def migrate(self, req, group_id, objtype, entity, body=None):
        """Entity变更agent"""
        body = body or {}
        jsonutils.schema_validate(body, self.MIGRATE)
        new = body.pop('new')
        body.update({'databases': True, 'chiefs': True})
        session = endpoint_session(readonly=True)
        query = model_query(session, AppEntity, filter=AppEntity.entity == entity)
        _entity = query.one()
        if _entity.objtype != objtype:
            raise InvalidArgument('Entity is not %s' % objtype)
        query = query.options(joinedload(AppEntity.areas, innerjoin=False))
        with entity_controller.migrate_with_out_data(common.NAME, entity, new,
                                                     dict(token=uuidutils.generate_uuid()),
                                                     drop_ports=True):
            eventlet.sleep(0.001)   # 等待可能的主从同步延迟
            _entity = query.one()
            entity_controller.post_create_entity(
                entity, common.NAME, objtype=objtype,
                status=_entity.status, opentime=_entity.opentime, group_id=group_id,
                areas=[dict(area_id=area.area_id, areaname=area.areaname, show_id=area.show_id)
                       for area in _entity.areas])
            LOG.info('Notify create entity in new agent success')
            return self.reset(req, group_id, objtype, entity, body)