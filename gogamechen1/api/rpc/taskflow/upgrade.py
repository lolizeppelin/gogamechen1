# -*- coding:utf-8 -*-
from simpleutil.config import cfg
from simpleutil.log import log as logging

from simpleflow.api import load
from simpleflow.types import failure
from simpleflow.storage import Connection
from simpleflow.patterns import unordered_flow as uf
from simpleflow.storage.middleware import LogBook
from simpleflow.engines.engine import ParallelActionEngine

from goperation.manager.rpc.agent import sqlite
from goperation.manager.rpc.agent.application.taskflow.middleware import EntityMiddleware
from goperation.manager.rpc.agent.application.taskflow import application
from goperation.manager.rpc.agent.application.taskflow.database import Database
from goperation.manager.rpc.agent.application.taskflow.base import StandardTask
from goperation.manager.rpc.agent.application.taskflow import pipe
from goperation.taskflow import common as task_common

from gogamechen1 import common

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


def upgrade_entity(appendpoint, entity, objfiles, timeout):
    pass
#     middleware = GogameMiddle(endpoint=appendpoint, entity=entity, objtype=objtype)
#
#     conf = CONF['%s.%s' % (common.NAME, objtype)]
#     _database = []
#     # format database to class
#     for subtype in databases:
#         database_id = databases[subtype]
#         schema = '%s_%s_%s_%d' % (common.NAME, objtype, subtype, entity)
#         postfix = '-%d' % entity
#         auth = dict(user=conf.get('%s_%s' % (subtype, 'user')) + postfix,
#                     passwd=conf.get('%s_%s' % (subtype, 'passwd')),
#                     ro_user=conf.get('%s_%s' % (subtype, 'ro_user')) + postfix,
#                     ro_passwd=conf.get('%s_%s' % (subtype, 'ro_passwd')),
#                     source='%s/%s' % (appendpoint.manager.ipnetwork.network,
#                                       appendpoint.manager.ipnetwork.netmask))
#         LOG.debug('Create schema %s in %d with auth %s' % (schema, database_id, str(auth)))
#         _database.append(GogameCreateDatabase(database_id=database_id, schema=schema,
#                                               character_set='utf8',
#                                               subtype=subtype,
#                                               host=None, port=None, **auth))
#
#     app = application.Application(middleware,
#                                   createtask=GogameAppCreate(middleware, timeout),
#                                   databases=_database)
#
#     book = LogBook(name='create_%s_%d' % (appendpoint.namespace, entity))
#     store = dict(objfile=objfile, chiefs=chiefs, download_timeout=timeout)
#     taskflow_session = sqlite.get_taskflow_session()
#     create_flow = pipe.flow_factory(taskflow_session, applications=[app, ], store=store,
#                                     db_flow_factory=create_db_flowfactory)
#     connection = Connection(taskflow_session)
#     engine = load(connection, create_flow, store=store,
#                   book=book, engine_cls=ParallelActionEngine)
#
#     try:
#         engine.run()
#     except Exception as e:
#         if LOG.isEnabledFor(logging.DEBUG):
#             LOG.exception('Task execute fail')
#         else:
#             LOG.error('Task execute fail, %s %s' % (e.__class__.__name__, str(e)))
#     finally:
#         connection.destroy_logbook(book.uuid)
#         for dberror in middleware.dberrors:
#             LOG.error(str(dberror))
#     return middleware

