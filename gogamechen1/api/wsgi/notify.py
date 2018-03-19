from simpleutil.config import cfg
from goperation.manager.notify import HttpNotify

from gogamechen1 import common

CONF = cfg.CONF


class FrontNotify(HttpNotify):

    def entity(self, group_id, objtype, entity, delete=False):
        params = {'group_id': group_id, 'objtype': objtype,
                  'entity': entity}
        if delete:
            params.setdefault('op', 'del')
        self._do('entity', replace={'params': params})

    def areas(self, group_id):
        self._do('areas', replace={'params': {'group_id': group_id}})

    def resource(self):
        self._do('resource')


FrontInfo = {}

if CONF[common.NAME].notify_resource_url:
    FrontInfo.setdefault('resource', dict(url=CONF[common.NAME].notify_resource_url, method='GET',
                                          timeout=10))

if CONF[common.NAME].notify_areas_url:
    FrontInfo.setdefault('areas', dict(url=CONF[common.NAME].notify_areas_url, method='GET',
                                       timeout=10))

if CONF[common.NAME].notify_entity_url:
    FrontInfo.setdefault('entity', dict(url=CONF[common.NAME].notify_entity_url, method='GET',
                                        timeout=10))

notify = FrontNotify(FrontInfo, delay=5)
