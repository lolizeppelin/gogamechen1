# -*- coding:utf-8 -*-
import time
from simpleutil.utils import cachetools
from simpleutil.common.exceptions import InvalidArgument

import goperation
from goperation.manager.api import get_cache

from gopcdn import common as cdncommon
from gopcdn.api.wsgi.resource import CdnResourceReuest

cdnresource_controller = CdnResourceReuest()


class ResourceTTLCache(cachetools.TTLCache):
    def __init__(self, maxsize, ttl):
        cachetools.TTLCache.__init__(self, maxsize, ttl)

    def expiretime(self, key):
        link = self._TTLCache__links[key]
        return link.expire


# CDNRESOURCE = {}
CDNRESOURCE = ResourceTTLCache(maxsize=1000, ttl=cdncommon.CACHETIME)


def map_resources(resource_ids):
    # 删除过期缓存
    CDNRESOURCE.expire()

    need = set(resource_ids)
    provides = set(CDNRESOURCE.keys())
    notmiss = need & provides

    if notmiss:
        cache_base = {}
        earliest = int(time.time())
        for resource_id in notmiss:
            update_at = int(time.time()) - CDNRESOURCE.expiretime(resource_id)
            if update_at < earliest:
                earliest = update_at
            cache_base[resource_id] = update_at

        cache = get_cache()
        scores = cache.zrangebyscore(name=cdncommon.CACHESETNAME,
                                     min=str(earliest), max='+inf',
                                     withscores=True, score_cast_func=int)
        if scores:
            for data in scores:
                resource_id = int(data[0])
                update_at = int(data[1])
                # may pop by other green thread
                # if resource_id in notmiss:
                try:
                    if update_at > cache_base[resource_id]:
                        CDNRESOURCE.pop(resource_id, None)
                except KeyError:
                    continue

    missed = need - set(CDNRESOURCE.keys())

    if missed:
        with goperation.tlock('gogamechen1-cdnresource'):
            resources = cdnresource_controller._shows(resource_ids=missed,
                                                      versions=True, domains=True, metadatas=True)
            for resource in resources:
                resource_id = resource.get('resource_id')
                agent_id = resource.get('agent_id')
                port = resource.get('port')
                internal = resource.get('internal')
                name = resource.get('name')
                etype = resource.get('etype')
                domains = resource.get('domains')
                versions = resource.get('versions')
                metadata = resource.get('metadata')
                if internal:
                    if not metadata:
                        raise ValueError('Agent %d not online, get domain entity fail' % agent_id)
                    hostnames = [metadata.get('local_ip')]
                else:
                    if not domains:
                        if not metadata:
                            raise ValueError('Agent %d not online get domain entity fail' % agent_id)
                        if metadata.get('external_ips'):
                            hostnames = metadata.get('external_ips')
                        else:
                            hostnames = [metadata.get('local_ip')]
                    else:
                        hostnames = domains
                schema = 'http'
                if port == 443:
                    schema = 'https'
                netlocs = []
                for host in hostnames:
                    if port in (80, 443):
                        netloc = '%s://%s' % (schema, host)
                    else:
                        netloc = '%s://%s:%d' % (schema, host, port)
                    netlocs.append(netloc)
                CDNRESOURCE.setdefault(resource_id, dict(name=name, etype=etype, agent_id=agent_id,
                                                         internal=internal, versions=versions,
                                                         netlocs=netlocs, port=port,
                                                         domains=domains))


def resource_cache_map(resource_id):
    """cache  resource info"""
    if resource_id not in CDNRESOURCE:
        map_resources(resource_ids=[resource_id, ])
    if resource_id not in CDNRESOURCE:
        raise InvalidArgument('Resource not exit')
    return CDNRESOURCE[resource_id]
