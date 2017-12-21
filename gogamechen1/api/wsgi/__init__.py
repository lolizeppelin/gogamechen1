from simpleutil.config import cfg
from gogamechen1.api.wsgi.config import register_opts

from gogamechen1 import common

CONF = cfg.CONF

register_opts(CONF.find_group(common.NAME))
