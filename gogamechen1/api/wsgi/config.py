from simpleutil.config import cfg
from simpleservice.ormdb.config import database_opts

CONF = cfg.CONF

resource_opts = [
    cfg.IntOpt('objfile_resource',
               help='Gopcdn resource for objfile'),
    cfg.IntOpt('package_resource',
               help='Gopcdn resource for packages'),
]



def register_opts(group):
    # database for gopdb
    CONF.register_opts(database_opts, group)
    CONF.register_opts(resource_opts, group)
