import time
import simpleservice
from websocket import create_connection

from simpleutil.config import cfg
from goperation import config

from goperation.api.client import ManagerClient

from gogamechen1.api.client import GogameChen1DBClient
from gogamechen1 import common
import os
from simpleutil.utils import digestutils
from goperation.manager import common as manager_common


a = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\goperation.conf'
b = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\gcenter.conf'
config.configure('test', [a, b])

# wsgi_url = '127.0.0.1'
wsgi_url = '172.31.0.110'
wsgi_port = 7999

httpclient = ManagerClient(wsgi_url, wsgi_port, timeout=30, token='938lhsaposg433tsdlfasjlgk938')

client = GogameChen1DBClient(httpclient)

def group_index_test():
    for r in client.groups_index()['data']:
        print r


def group_create_test():
    print client.groups_create(name='test', desc='test group')


def group_show_test(group_id):
    print client.group_show(group_id=group_id, detail=True)


def group_map_test(group_id):
    print client.group_maps(group_id=group_id)


def group_delete(group_id):
    print client.group_delete(group_id=group_id)


def game_index():
    for game in client.games_index(group_id=1, body={'detail': True})['data']:
        print game

def game_show(entity):
    print client.game_show(group_id=1, entity=entity)


def game_status(entitys):
    print client.game_status(group_id=1, entitys=entitys, body=dict(request_time=int(time.time())))


def game_stop(entitys):
    print client.game_stop(group_id=1, entitys=entitys, body=dict(request_time=int(time.time()),
                                                                  kill=True))


def game_flushconf(entitys):
    print client.game_flushconfig(group_id=1, entitys=entitys,
                                  body={'request_time': int(time.time()),
                                        common.GMSERVER: True})


def game_create():
    print client.games_create(group_id=1, body={'objfile': {'subtype': 'appfile', 'version': '20180104.002'},
                                                'opentime': int(time.time())})

def crosss_create():
    print client.crosss_create(group_id=1, body={'objfile': {'subtype': 'appfile',
                                                             'version': '20180104.001'}})

def cross_delete(entity):
    print client.cross_delete(group_id=1, entity=entity, clean='delete')


def cross_show(entity):
    print client.cross_show(group_id=1, entity=entity, detail=True)


def gm_create():
    print client.gms_create(group_id=1, body={'objfile': {'subtype': 'appfile', 'version': '20180104.001'}})

def gm_delete(entity):
    print client.gm_delete(group_id=1, entity=entity, clean='delete')


def gm_flushconf(entity):
    print client.gm_flushconfig(group_id=1, entity=entity, body={'request_time': int(time.time())})


def gm_show(entity):
    print client.gm_show(group_id=1, entity=entity, detail=True)


def game_start(entitys):
    print client.game_start(group_id=1, entitys=entitys, body={'request_time': int(time.time()),})


def game_merge(appfile, group_id, entitys):
    try:
        r = client.merge_entitys(body={'entitys': entitys,
                                       'group_id': group_id,
                                       'appfile': appfile})
    except Exception as e:
        print e.message
        try:
            print e.resone
        except:
            pass
    else:
        print r


def continue_merge(uuid):
    try:
        r = client.continue_merge(uuid)
    except Exception as e:
        print e.message
        try:
            print e.resone
        except:
            pass
    else:
        print r



# group_create_test()
# group_index_test()
# group_show_test(1)
# group_map_test(1)

# group_delete(1)

# game_index()
# game_create()
# game_show(3)
# game_status('all')

# crosss_create()
# cross_show(entity=2)
# cross_delete(5)

# gm_create()
# gm_delete(entity=4)
# gm_show(entity=1)


# print client.quotes(endpoint='gogamechen1', entitys=[1,2,3,4,5])

# print client.reset(group_id=1, objtype='publicsvr', entity=5)

# game_start(entitys='all')
# game_flushconf(entitys='all')
# game_status(entitys='all')
# game_stop(entitys='all')

# gm_flushconf(entity='all')


# print client.async_show(request_id='70b979ef-e3be-4ed9-8e1c-16484019fe3c', body={'details': True})
# game_merge('ec27a2dede2df74dfa23eb8c449129a0', 97, [3, 4])
continue_merge('541390d4-5403-4391-9633-e577301b0fc2')
