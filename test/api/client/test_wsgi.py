import time
import simpleservice

from simpleutil.config import cfg
from goperation import config

from goperation.api.client import ManagerClient

from gogamechen1.api.client import GogameChen1DBClient
from gogamechen1 import common

from goperation.manager import common as manager_common


a = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\goperation.conf'
b = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\gcenter.conf'
config.configure('test', [a, b])

# wsgi_url = '127.0.0.1'
wsgi_url = '172.31.0.110'
wsgi_port = 7999

httpclient = ManagerClient(wsgi_url, wsgi_port, timeout=30)

client = GogameChen1DBClient(httpclient)


def file_create():
    cross = {'address': 'http://172.17.0.3/crosserver.zip',
                                      'size': 3640656,
                                      'md5': '37a92e3670793d9fe7d7b4166405b7c0',
                                      'crc32': '2328264931'}

    gm = {'address': 'http://172.17.0.3/gmserver.zip',
          'size': 3810087, 'md5': 'ccfccde94771f8a383055ae0ca54f395', 'crc32': '3345614830'}

    game = {'address': 'http://172.17.0.3/gameserver.zip',
            'size': 21363437, 'md5': '4aef586427aa0fab051d19c930a87c0b', 'crc32': '3527425352'}

    print client.objfile_create(objtype=common.GAMESERVER, subtype=common.APPFILE,
                                version='20180104.001',
                                body=game)

def file_index():
    print client.objfiles_index()

def send_file(uuid):
    print client.send_file_to_agents(agent_id=6, file_id=uuid,
                                     body={'request_time': int(time.time())})

def group_index_test():
    print client.groups_index()

def group_create_test():
    print client.groups_create(name='test', desc='test group')

def group_show_test(group_id):
    print client.group_show(group_id=group_id, detail=True)


def group_map_test(group_id):
    print client.group_maps(group_id=group_id)


def game_index():
    print client.games_index(group_id=1)

def game_show(entity):
    print client.game_show(group_id=1, entity=entity)


def crosss_create():
    print client.crosss_create(group_id=1, body={'impl': 'local',
                                                 'objfile': {'subtype': 'appfile',
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


# file_create()
# file_index()
# send_file(uuid='a1ed7026-c7a4-4bde-b031-4b4106386c2e')

# group_create_test()
# group_index_test()
# group_show_test(1)
# group_map_test(1)


# game_index()
# game_show(1)

# crosss_create()
# cross_show(entity=1)
# cross_delete(1)

# gm_create()
# gm_delete(entity=2)
# gm_show(entity=2)


print client.quotes(endpoint='gogamechen1', entitys=[1,2,3,4,5])