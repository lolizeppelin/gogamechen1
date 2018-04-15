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
        cachetools.TTLCache.__init__(self, maxsize, ttl, timer=time.time)

    def expiretime(self, key):
        """这个接口返回未来将在这个时间点过期"""
        link = self._TTLCache__links[key]
        return link.expire

CDNRESOURCE = ResourceTTLCache(maxsize=1000, ttl=cdncommon.CACHETIME)


def map_resources(resource_ids):
    # 删除过期缓存
    CDNRESOURCE.expire()

    need = set(resource_ids)
    provides = set(CDNRESOURCE.keys())
    notmiss = need & provides

    # 有资源在进程缓存字典中
    if notmiss:
        caches_time_dict = {}
        # 本地最旧缓存时间点
        time_point = int(time.time())
        for resource_id in notmiss:
            # 获取单个资源本地缓存时间点
            cache_on = int(CDNRESOURCE.expiretime(resource_id)) - cdncommon.CACHETIME
            if cache_on < time_point:
                time_point = cache_on
            caches_time_dict[resource_id] = cache_on
        cache = get_cache()
        scores = cache.zrangebyscore(name=cdncommon.CACHESETNAME,
                                     min=str(time_point - 3), max='+inf',
                                     withscores=True, score_cast_func=int)
        if scores:
            for data in scores:
                resource_id = int(data[0])
                cache_on = int(data[1])
                # redis中缓存时间点超过本地缓存时间点
                # 弹出本地缓存
                try:
                    # 保险做法本地缓存时间回退3秒
                    if cache_on > caches_time_dict[resource_id] - 3:
                        CDNRESOURCE.pop(resource_id, None)
                except KeyError:
                    continue
        caches_time_dict.clear()
    # 没有本地缓存的资源数量
    missed = need - set(CDNRESOURCE.keys())
    if missed:
        # 重新从数据库读取
        with goperation.tlock('gogamechen1-cdnresource'):
            resources = cdnresource_controller.list(resource_ids=missed,
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


def resource_cache_map(resource_id, flush=True):
    """cache  resource info"""
    if flush:
        map_resources(resource_ids=[resource_id, ])
    if resource_id not in CDNRESOURCE:
        raise InvalidArgument('Resource not exit')
    return CDNRESOURCE[resource_id]
