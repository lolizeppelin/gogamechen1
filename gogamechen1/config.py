from simpleutil.config import cfg

CONF = cfg.CONF


game_opts = [
    cfg.StrOpt('defaultpass',
               help='Game app entity database base password prefix',
               required=True,
               secret=True)
]


def register_opts(group):
    # database for gopdb
    CONF.register_opts(game_opts, group)