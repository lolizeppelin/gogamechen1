# -*- coding:utf-8 -*-
from sqlalchemy.sql import and_

from simpleservice.ormdb.api import model_query

from goperation.manager.wsgi.entity.controller import EntityReuest

from gogamechen1 import common
from gogamechen1.api import endpoint_session
from gogamechen1.api import exceptions
from gogamechen1.models import AppEntity

entity_controller = EntityReuest()


def gmurl(req, group_id, interface):
    session = endpoint_session(readonly=True)
    query = model_query(session, AppEntity,
                        filter=and_(AppEntity.objtype == common.GMSERVER,
                                    AppEntity.group_id == group_id))
    gm = query.one_or_none()
    if not gm or gm.status != common.OK:
        raise exceptions.GmSvrHttpError('GM is none or not active' % common.GMSERVER)
    entityinfo = entity_controller.show(req=req, entity=gm.entity,
                                        endpoint=common.NAME,
                                        body={'ports': True})['data'][0]
    port = entityinfo.get('ports')[0]
    metadata = entityinfo.get('metadata')
    if not metadata:
        raise exceptions.GmSvrHttpError('%s.%d is off line, can not stop by %s' % (gm.objtype, gm.entity, gm.objtype))
    ipaddr = metadata.get('local_ip')
    url = 'http://%s:%d/%s' % (ipaddr, port, interface)
    return url
