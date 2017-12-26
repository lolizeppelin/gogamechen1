import copy
from simpleutil.config import cfg

from gogamechen1 import common

CONF = cfg.CONF


datadb_opts = [
    cfg.StrOpt('datadb_user',
               help='data db rw user name'),
    cfg.StrOpt('datadb_passwd',
               required=True,
               secret=True,
               help='data db rw user passwd'),
    cfg.StrOpt('datadb_ro_user',
               help='data db ro user name'),
    cfg.IntOpt('datadb_ro_passwd',
               required=True,
               secret=True,
               help='data db ro user passwd'),
]


logdb_opts = [
    cfg.StrOpt('logdb_user',
               help='logdb db rw user name'),
    cfg.StrOpt('logdb_passwd',
               required=True,
               secret=True,
               help='logdb db rw user passwd'),
    cfg.StrOpt('logdb_ro_user',
               help='logdb db ro user name'),
    cfg.IntOpt('logdb_ro_passwd',
               required=True,
               secret=True,
               help='logdb db ro user passwd'),
]


gameserver_group = cfg.OptGroup(name='%s.game' % common.NAME)
gmserver_group = cfg.OptGroup(name='%s.gm' % common.NAME)
crossserver_group = cfg.OptGroup(name='%s.cross' % common.NAME)


def list_game_opts():
    _datadb_opts = copy.deepcopy(datadb_opts)
    _logdb_opts = copy.deepcopy(logdb_opts)
    cfg.set_defaults(_datadb_opts, datadb_user='gogamedb-rw', datadb_ro_user='gogamedb-ro')
    cfg.set_defaults(_logdb_opts, logdb_user='gogamelog-rw', logdb_ro_user='gogamelog-ro')
    return  _datadb_opts + _logdb_opts

def game_register_opts(group):
    # database for gameserver
    CONF.register_opts(list_game_opts(), group)


def list_gm_opts():
    _datadb_opts = copy.deepcopy(datadb_opts)
    cfg.set_defaults(_datadb_opts, datadb_user='gogmdb-rw', datadb_ro_user='gogmdb-ro')
    return _datadb_opts


def gm_register_opts(group):
    # database for gameserver
    CONF.register_opts(list_gm_opts(), group)


def list_cross_opts():
    _datadb_opts = copy.deepcopy(datadb_opts)
    cfg.set_defaults(_datadb_opts, datadb_user='gocross-rw', datadb_ro_user='gocross-ro')
    return _datadb_opts


def cross_register_opts(group):
    # database for cross server
    CONF.register_opts(list_cross_opts(), group)


def register_opts():
    game_register_opts(gameserver_group)
    gm_register_opts(gmserver_group)
    cross_register_opts(crossserver_group)
