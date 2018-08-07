# -*- coding:utf-8 -*-
from sqlalchemy.orm import joinedload
from simpleutil.log import log as logging
from simpleutil.utils import argutils
from simpleutil.config import cfg

from simpleservice.ormdb.api import model_query


from goperation.manager.utils import resultutils
from goperation.manager.wsgi.port.controller import PortReuest
from goperation.manager.wsgi.entity.controller import EntityReuest

from gopdb.api.wsgi.controller import SchemaReuest
from gopdb.api.wsgi.controller import DatabaseReuest

from gopcdn.api.wsgi.resource import CdnQuoteRequest
from gopcdn.api.wsgi.resource import CdnResourceReuest

from gogamechen1 import common
from gogamechen1.api import endpoint_session

from gogamechen1.models import AppEntity

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
    """async internal function"""

    MERGEAPPENTITYS = {'type': 'object',
                       'required': [common.APPFILE, 'entitys', 'group_id'],
                       'properties': {
                           'entitys': {'type': 'array',
                                       'items': {'type': 'integer', 'minimum': 2},
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
                                                          show_id=area.show_id,
                                                          areaname=area.areaname)
                                                     for area in _entity.areas],
                                              objtype=_entity.objtype) for _entity in query])
