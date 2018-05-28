# -*- coding:utf-8 -*-
import six
import webob.exc

from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_

from simpleutil.common.exceptions import InvalidArgument
from simpleutil.log import log as logging
from simpleutil.utils import jsonutils
from simpleutil.utils import argutils
from simpleutil.utils import singleton
from simpleutil.config import cfg

from simpleservice.ormdb.api import model_query
from simpleservice.ormdb.api import model_count_with_key
from simpleservice.ormdb.exceptions import DBDuplicateEntry
from simpleservice.rpc.exceptions import AMQPDestinationNotFound
from simpleservice.rpc.exceptions import MessagingTimeout
from simpleservice.rpc.exceptions import NoSuchMethod

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

from gopdb.api.wsgi.controller import SchemaReuest
from gopdb.api.wsgi.controller import DatabaseReuest

from gopcdn.api.wsgi.resource import CdnQuoteRequest
from gopcdn.api.wsgi.resource import CdnResourceReuest

from gogamechen1 import common
from gogamechen1 import utils
from gogamechen1.api import endpoint_session

from gogamechen1.models import Group
from gogamechen1.models import AppEntity
from gogamechen1.models import GameArea

from .entity.curd import AppEntityCURDRequest
from .entity.async import AppEntityAsyncReuest
from .entity.sync import AppEntitySyncReuest
from .entity.internal import AppEntityInternalReuest


LOG = logging.getLogger(__name__)

FAULT_MAP = {
    InvalidArgument: webob.exc.HTTPClientError,
    NoSuchMethod: webob.exc.HTTPNotImplemented,
    AMQPDestinationNotFound: webob.exc.HTTPServiceUnavailable,
    MessagingTimeout: webob.exc.HTTPServiceUnavailable,
    RpcResultError: webob.exc.HTTPInternalServerError,
    CacheStoneError: webob.exc.HTTPInternalServerError,
    RpcPrepareError: webob.exc.HTTPInternalServerError,
    NoResultFound: webob.exc.HTTPNotFound,
    MultipleResultsFound: webob.exc.HTTPInternalServerError
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
    AREA = {'type': 'object',
            'required': ['area_id'],
            'properties': {
                'area_id': {'type': 'integer', 'minimum': 1, 'description': '游戏区服ID'},
                'show_id': {'type': 'integer', 'minimum': 1, 'description': '游戏区服显示ID'},
                'areaname': {'type': 'string', 'description': '游戏区服显示名称'}}
            }

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

    def area(self, req, group_id, body=None):
        body = body or {}
        try:
            group_id = int(group_id)
        except (TypeError, ValueError):
            raise InvalidArgument('Group id value error')
        area_id = body.get('area_id')
        show_id = body.get('show_id')
        areaname = body.get('areaname')
        if not areaname and not show_id:
            raise InvalidArgument('No value change')
        rpc = get_client()
        session = endpoint_session()
        query = model_query(session, GameArea, filter=GameArea.area_id == area_id)
        with session.begin():
            area = query.one_or_none()
            if not area:
                raise InvalidArgument('No area found')
            if area.group_id != group_id:
                raise InvalidArgument('Area group not %d' % group_id)
            entityinfo = entity_controller.show(req=req, entity=area.entity,
                                                endpoint=common.NAME,
                                                body={'ports': False})['data'][0]
            agent_id = entityinfo['agent_id']
            metadata = entityinfo['metadata']
            if not metadata:
                raise InvalidArgument('Agent is off line, can not reset entity')
            if areaname:
                if model_count_with_key(session, GameArea,
                                        filter=and_(GameArea.group_id == group_id,
                                                    GameArea.areaname == areaname)):
                    raise InvalidArgument('Area name duplicate in group %d' % group_id)
                area.areaname = areaname
            if show_id:
                area.show_id = show_id
            target = targetutils.target_agent_by_string(metadata.get('agent_type'), metadata.get('host'))
            target.namespace = common.NAME
            finishtime, timeout = rpcfinishtime()
            rpc_ret = rpc.call(target, ctxt={'finishtime': finishtime, 'agents': [agent_id, ]},
                               msg={'method': 'change_entity_area',
                                    'args': dict(entity=area.entity,
                                                 area_id=area.area_id,
                                                 show_id=area.show_id,
                                                 areaname=area.areaname)},
                               timeout=timeout)
            if not rpc_ret:
                raise RpcResultError('change entity area result is None')
            if rpc_ret.get('resultcode') != manager_common.RESULT_SUCCESS:
                raise RpcResultError('change entity area fail %s' % rpc_ret.get('result'))
            session.flush()
        return resultutils.results(result='change group areas success')

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
                            platform=appentity.platform,
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
class AppEntityReuest(AppEntityCURDRequest,
                      AppEntityAsyncReuest,
                      AppEntitySyncReuest,
                      AppEntityInternalReuest):
    """Appentity request class"""
