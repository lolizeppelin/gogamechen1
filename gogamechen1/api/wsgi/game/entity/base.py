# -*- coding:utf-8 -*-
import six

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.config import cfg

from goperation.manager.wsgi.contorller import BaseContorller
from goperation.manager.wsgi.entity.controller import EntityReuest

from gopdb.api.wsgi.controller import DatabaseReuest

from gogamechen1 import common

from gogamechen1.models import AreaDatabase

LOG = logging.getLogger(__name__)

entity_controller = EntityReuest()
database_controller = DatabaseReuest()

CONF = cfg.CONF


class AppEntityReuestBase(BaseContorller):
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
        LOG.debug('Auto select agent %d' % chioces[0])
        return chioces[0]

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
