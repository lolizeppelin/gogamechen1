import time
import eventlet
import contextlib

from redis.exceptions import WatchError
from redis.exceptions import ResponseError

from simpleutil.utils import argutils

from goperation.manager.gdata import GlobalData
from goperation.manager.exceptions import AllocLockTimeout


class GoGameLock(object):

    GAGAME_GROUP_KEY = '-'.join([GlobalData.PREFIX, 'gogamechen1', '%d'])

    def __init__(self, gdata):
        if not isinstance(gdata, GlobalData):
            raise TypeError('httpclient must class of ManagerClient')
        self.gdata = gdata

    def __getattr__(self, attrib):
        if not hasattr(self.gdata, attrib):
            raise AttributeError('%s has no attrib %s' % (self.__class__.__name__, attrib))
        return getattr(self.gdata, attrib)

    @contextlib.contextmanager
    def grouplock(self, group):
        overtime = self.alloctime + int(time.time()*1000)
        client = self.client
        key = self.GAGAME_GROUP_KEY % group
        while True:
            if client.set(key, self.locker, nx=True):
                break
            if int(time.time()*1000) > overtime:
                raise AllocLockTimeout('Alloc key %s timeout' % key)
            eventlet.sleep(0.003)
        try:
            yield group
        finally:
            self.garbage_key_collection(key)

    @contextlib.contextmanager
    def arealock(self, group, areas):
        areas = argutils.map_to_int(areas)
        overtime = self.alloctime + int(time.time()*1000)
        key = self.GAGAME_GROUP_KEY % group
        client = self.client
        while True:
            wpipe = None
            try:
                while int(time.time()*1000) < overtime:
                    with client.pipeline() as pipe:
                        for _id in areas:
                            pipe.sismember(key, str(_id))
                        results = pipe.execute()
                    if all([True if not result else False for result in results]):
                        break
                    else:
                        eventlet.sleep(0.01)
                wpipe = client.pipeline()
                wpipe.watch(key)
                wpipe.multi()
                wpipe.sadd(key, *map(str, areas))
                wpipe.execute()
                break
            except WatchError:
                if int(time.time()*1000) > overtime:
                    raise AllocLockTimeout('Lock areas timeout')
            except ResponseError as e:
                if not e.message.startswith('WRONGTYPE'):
                    if int(time.time() * 1000) > overtime:
                        raise AllocLockTimeout('Lock areas timeout')
                    raise
            finally:
                if wpipe:
                    wpipe.reset()
        try:
            yield areas
        finally:
            self.garbage_member_collection(key, map(str, areas))
