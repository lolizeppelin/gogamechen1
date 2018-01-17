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

httpclient = ManagerClient(wsgi_url, wsgi_port, timeout=30)

client = GogameChen1DBClient(httpclient)


def file_create(path):
    md5 = digestutils.filemd5(path)
    crc32 = digestutils.filecrc32(path)
    size = os.path.getsize(path)
    ext = os.path.split(path)[1][1:]
    body = {'size': size,
            'crc32': crc32,
            'md5': md5,
            'ext': os.path.splitext(path)[1][1:],
            }

    ret = client.objfile_create(common.GMSERVER, 'appfile', '1001', body=body)['data'][0]

    print 'create cdn result %s' % str(ret)

    uri = ret.get('uri')
    import time
    time.sleep(0.1)
    ws = create_connection("ws://%s:%d" % (uri.get('ipaddr'), uri.get('port')),
                           subprotocols=["binary"])
    print "connect websocket success"
    with open(path, 'rb') as f:
        while True:
            buffer = f.read(4096)
            if buffer:
                ws.send(buffer)
            else:
                print 'file send finish'
                break


def file_index():
    print client.objfiles_index()


def send_file(agent_id, uuid):
    print client.send_file_to_agents(agent_id=agent_id, file_id=uuid,
                                     body={'request_time': int(time.time())})


def group_index_test():
    print client.groups_index()


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


def gm_show(entity):
    print client.gm_show(group_id=1, entity=entity, detail=True)


path = r'C:\Users\loliz_000\Desktop\zhuomian5\charge.dat'

file_create(path)

# file_index()
# send_file(agent_id=6, uuid='ed3c683c-64a7-45d3-b149-c156ce3af508')

# group_create_test()
# group_index_test()
# group_show_test(1)
# group_map_test(1)

# group_delete(1)

# game_index()
# game_create()
# game_show(3)

# crosss_create()
# cross_show(entity=1)
# cross_delete(5)

# gm_create()
# gm_delete(entity=4)
# gm_show(entity=2)


# print client.quotes(endpoint='gogamechen1', entitys=[1,2,3,4,5])

# print client.reset(group_id=1, objtype='publicsvr', entity=5)
