from simpleutil.config import cfg
from simpleservice.ormdb.config import database_opts

CONF = cfg.CONF




game_opts = [
    cfg.StrOpt('defaultpass',
               help='Game app entity database base password prefix',
               required=True,
               secret=True)
]



def register_opts(group):
    # database for gopdb
    CONF.register_opts(database_opts, group)
    CONF.register_opts(database_opts, group)