from simpleutil.config import cfg
from goperation.manager.notify import HttpNotify

from gogamechen1 import common

CONF = cfg.CONF


class FrontNotify(HttpNotify):
    def areas(self, group_id):
        self._do('appentity', replace={'params': {'group_id': group_id}})

    def resource(self):
        self._do('resource')


FrontInfo = {}

if CONF[common.NAME].notify_resource_url:
    FrontInfo.setdefault('resource', dict(url=CONF[common.NAME].notify_resource_url, method='POST'))

if CONF[common.NAME].notify_resource_url:
    FrontInfo.setdefault('areas', dict(url=CONF[common.NAME].notify_areas_url, method='POST'))

notify = FrontNotify(FrontInfo)
