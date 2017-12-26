#!/usr/bin/python
import logging

from simpleutil.config import cfg
from simpleservice.ormdb.tools.utils import database_init_opts

from gogamechen1.cmd.db import utils


def main():
    logging.basicConfig(level=logging.WARN)
    conf = cfg.ConfigOpts()
    conf.register_cli_opts(database_init_opts)
    conf()
    utils.init_gopdb(db_info=dict(user=conf.user,
                                  passwd=conf.passwd,
                                  host=conf.host,
                                  port=str(conf.port),
                                  schema=conf.schema))


if __name__ == '__main__':
    main()