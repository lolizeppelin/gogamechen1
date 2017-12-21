from simpleutil.log import log as logging
from simpleutil.config import cfg

from simpleservice.ormdb.api import MysqlDriver

from goperation import lock

from gogamechen1 import common

CONF = cfg.CONF

DbDriver = None


LOG = logging.getLogger(__name__)

def init_endpoint_session():
    global DbDriver
    if DbDriver is None:
        with lock.get('mysql-gogamechen1'):
            if DbDriver is None:
                LOG.info("Try connect database for gopdb")
                mysql_driver = MysqlDriver(common.NAME,
                                           CONF[common.NAME])
                mysql_driver.start()
                DbDriver = mysql_driver
    else:
        LOG.warning("Do not call init_endpoint_session more then once")


def endpoint_session(readonly=False):
    if DbDriver is None:
        init_endpoint_session()
    return DbDriver.get_session(read=readonly,
                                autocommit=True,
                                expire_on_commit=False)