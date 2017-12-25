from simpleutil.config import cfg
from simpleservice.ormdb.config import database_opts

CONF = cfg.CONF


def register_opts(group):
    # database for gopdb
    CONF.register_opts(database_opts, group)