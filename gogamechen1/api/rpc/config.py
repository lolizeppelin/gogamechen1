import re
import copy
from simpleutil.config import cfg

from gogamechen1 import common

CONF = cfg.CONF

REGEXUSER = re.compile('^%s$' % common.REGEXUSER)
REGEXPASS = re.compile('^%s$' % common.REGEXPASS)

agent_opts = [
    cfg.IntOpt('agent_affinity',
               choices=[1, 2, 3, 4, 5, 6, 7],
               default=1,
               help='agent appcaclition affinity bitwise, '
                    '[gamesvr:1] [publicsvr:2] [gmsvr:4] '
                    '[publicsvr&gmsvr:6] '
                    '[gamesvr&publicsvr&gmsvr:7] and so on'),
    cfg.IntOpt('periodic_interval',
               default=3,
               min=0, max=10,
               help='Entity process periodic check interval by min'),
    cfg.IntOpt('auto_restart_times',
               default=1,
               min=0, max=5,
               help='Auto start entity process times when entity dead, '
                    '0 means do not auto restart dead entity process'),
    cfg.IntOpt('pop_from_deads',
               default=180,
               min=30, max=600,
               help='Entity will remove from auto restart periodic task dead process list after seconds')
]

sources_opts = [
    cfg.StrOpt('source',
               default=None,
               help='databse rw user source privilege limit'),
    cfg.StrOpt('rosource',
               default=None,
               help='databse ro user source privilege limit'),
]


datadb_opts = [
    cfg.StrOpt('datadb_user',
               # regex=REGEXUSER,
               help='data db rw user name'),
    cfg.StrOpt('datadb_passwd',
               # regex=REGEXPASS,
               secret=True,
               help='data db rw user passwd'),
    cfg.StrOpt('datadb_ro_user',
               # regex=REGEXUSER,
               help='data db ro user name'),
    cfg.StrOpt('datadb_ro_passwd',
               # regex=REGEXPASS,
               secret=True,
               help='data db ro user passwd'),
]


logdb_opts = [
    cfg.StrOpt('logdb_user',
               # regex=REGEXUSER,
               help='logdb db rw user name'),
    cfg.StrOpt('logdb_passwd',
               # regex=REGEXPASS,
               secret=True,
               help='logdb db rw user passwd'),
    cfg.StrOpt('logdb_ro_user',
               # regex=REGEXUSER,
               help='logdb db ro user name'),
    cfg.StrOpt('logdb_ro_passwd',
               # regex=REGEXPASS,
               secret=True,
               help='logdb db ro user passwd'),
]


gameserver_group = cfg.OptGroup(name='%s.%s' % (common.NAME, common.GAMESERVER),
                                title='gameserver group')
gmserver_group = cfg.OptGroup(name='%s.%s' % (common.NAME, common.GMSERVER),
                              title='gm server group')
crossserver_group = cfg.OptGroup(name='%s.%s' % (common.NAME, common.CROSSSERVER),
                                 title='cross server group')


def list_game_opts():
    _sources_opts = copy.deepcopy(sources_opts)
    _datadb_opts = copy.deepcopy(datadb_opts)
    _logdb_opts = copy.deepcopy(logdb_opts)
    cfg.set_defaults(_datadb_opts, datadb_user='gogamedb-rw', datadb_ro_user='gogamedb-ro')
    cfg.set_defaults(_logdb_opts, logdb_user='gogamelog-rw', logdb_ro_user='gogamelog-ro')
    return _sources_opts + _datadb_opts + _logdb_opts


def game_register_opts(group):
    # database for gameserver
    CONF.register_opts(list_game_opts(), group)


def list_gm_opts():
    _sources_opts = copy.deepcopy(sources_opts)
    _datadb_opts = copy.deepcopy(datadb_opts)
    cfg.set_defaults(_datadb_opts, datadb_user='gogmdb-rw', datadb_ro_user='gogmdb-ro')
    return _sources_opts + _datadb_opts


def gm_register_opts(group):
    # database for gameserver
    CONF.register_opts(list_gm_opts(), group)


def list_cross_opts():
    _sources_opts = copy.deepcopy(sources_opts)
    _datadb_opts = copy.deepcopy(datadb_opts)
    cfg.set_defaults(_datadb_opts, datadb_user='gocross-rw', datadb_ro_user='gocross-ro')
    return _sources_opts + _datadb_opts


def cross_register_opts(group):
    # database for cross server
    CONF.register_opts(list_cross_opts(), group)


def register_opts():
    game_register_opts(gameserver_group)
    gm_register_opts(gmserver_group)
    cross_register_opts(crossserver_group)


def list_agent_opts():
    return agent_opts


def list_cdn_opts():
    from gopcdn.plugin.alias.config import alias_opts

    for opt in alias_opts:
        if opt.name == 'aliases':
            opt.default = ['%s.plugin.alias.Alias' % common.NAME]
            break

    return alias_opts
