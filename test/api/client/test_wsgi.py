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
    print client.crosss_create(group_id=1)

def cross_delete(entity):
    print client.cross_delete(group_id=1, entity=entity, clean='delete')


def cross_show(entity):
    print client.cross_show(group_id=1, entity=entity, detail=True)


def gm_create():
    print client.gms_create(group_id=1)

def gm_delete(entity):
    print client.gm_delete(group_id=1, entity=entity, clean='delete')

def gm_show(entity):
    print client.gm_show(group_id=1, entity=entity, detail=True)

# group_create_test()
# group_index_test()
# group_show_test(1)
# group_map_test(1)


# game_index()
# game_show(1)

# crosss_create()
# cross_show(entity=1)
# cross_delete(1)


gm_create()
# gm_delete(entity=2)
# gm_show(entity=2)