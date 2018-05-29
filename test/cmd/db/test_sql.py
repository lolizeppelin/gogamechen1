# -*- coding:utf-8 -*-
from sqlalchemy.sql import and_
from sqlalchemy import func

from simpleutil.log import log as logging

from simpleutil.config import cfg

from goperation import config

from gogamechen1.api import endpoint_session

from gogamechen1.models import AppEntity
from gogamechen1.models import GameArea
from gogamechen1 import common

LOG = logging.getLogger(__name__)

CONF = cfg.CONF

group_id = 97
platform = 'ios'

a = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\goperation.conf'
b = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\gcenter.conf'
c = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\endpoints\gogamechen1.server.conf'

from goperation.manager import common as manager_common
from goperation import config as goperation_config

# create a new project and group named gcenter
name = manager_common.SERVER
# init goperation config
main_group = goperation_config.configure(name, [a, b, c])
from simpleservice.ormdb.config import database_opts

endpoint_group = cfg.OptGroup(common.NAME, title='endpopint of %s' % common.NAME)
CONF.register_group(endpoint_group)
CONF.register_opts(database_opts, endpoint_group)

session = endpoint_session(readonly=True)

query = session.query(func.max(GameArea.show_id)).select_from(AppEntity)
query = query.join(AppEntity.areas, isouter=True)
query = query.filter(and_(AppEntity.group_id == group_id, AppEntity.platform == platform))
last_show_id = query.scalar() or 0

print last_show_id
