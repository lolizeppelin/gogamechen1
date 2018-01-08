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


class GogameMiddle(EntityMiddleware):

    def __init__(self, entity, endpoint, objtype):
        super(GogameMiddle, self).__init__(entity, endpoint)
        self.objtype = objtype
        self.databases = {}
        self.dberrors = []
        self.waiter = None


class GogameCreateDatabase(Database):

    def __init__(self, **kwargs):
        super(GogameCreateDatabase, self).__init__(backup=None, update=None, **kwargs)
        self.database_id = kwargs['database_id']
        self.source = kwargs['source']
        self.subtype = kwargs['subtype']
        self.ro_user = kwargs['ro_user']
        self.ro_passwd = kwargs['ro_passwd']


class GogameDatabaseCreateTask(StandardTask):

    @property
    def taskname(self):
        return self.__class__.__name__ + '-' + self.database.subtype


    def __init__(self, middleware, database):
        self.database = database
        super(GogameDatabaseCreateTask, self).__init__(middleware)

    def execute(self):
        appendpoint = self.middleware.reflection()
        # 创建并绑定数据库
        auth = dict(user=self.database.user, passwd=self.database.passwd,
                    ro_user=self.database.ro_user, ro_passwd=self.database.ro_passwd,
                    source=self.database.source)
        # 亲和性数值
        affinity = common.DBAFFINITYS[self.middleware.objtype][self.database.subtype]
        dbresult = appendpoint.client.schemas_create(self.database.database_id,
                                                     body={'schema': self.database.schema,
                                                           'affinity': affinity,
                                                           'auth': auth,
                                                           'bond': {'entity': self.middleware.entity,
                                                                    'endpoint': common.NAME}})['data'][0]
        # 设置返回结果
        self.middleware.databases.setdefault(self.database.subtype,
                                             dict(schema=self.database.schema,
                                                  database_id=self.database.database_id,
                                                  quote_id=dbresult.get('quote_id'),
                                                  host=dbresult.get('host'),
                                                  port=dbresult.get('port'),
                                                  user=self.database.user,
                                                  passwd=self.database.passwd,
                                                  ro_user=self.database.ro_user,
                                                  ro_passwd=self.database.ro_passwd,
                                                  character_set=self.database.character_set,
                                                  collation_type=self.database.collation_type))

    def revert(self, *args, **kwargs):
        LOG.info(args)
        LOG.info(kwargs)
        result = kwargs.get('result') or args[0]
        super(GogameDatabaseCreateTask, self).revert(result, **kwargs)
        if isinstance(result, failure.Failure):
            LOG.error('Create schema %s on %d fail' % (self.database.schema,
                                                       self.database.database_id))
            return
        # 弹出返回结果, 解绑并删除
        dbresult = self.middleware.database.pop(self.database.subtype)
        schema = dbresult.get('schema')
        database_id = dbresult.get('database_id')
        unquotes = [dbresult.get('quote_id')]
        try:
            self.middleware.reflection().client.schemas_delete(database_id=database_id,
                                                               schema=schema,
                                                               body={'unquotes': unquotes})
        except Exception as e:
            self.middleware.dberrors.append(dict(database_id=database_id, schema=schema, unquotes=unquotes,
                                                 reason='%s :%s' % (e.__class__.__name__, e.message)))
        else:
            self.middleware.set_return(self.taskname, task_common.REVERTED)


def create_db_flowfactory(app, store):
    # 数据库创建工作流生成
    middleware = app.middleware
    entity = middleware.entity
    objtype = middleware.objtype
    uflow = uf.Flow('create_%s_%s_%d' % (common.NAME, objtype, entity))
    for database in app.databases:
        uflow.add(GogameDatabaseCreateTask(middleware, database))
    return uflow


class GogameAppCreate(application.AppCreateBase):

    def __init__(self, middleware, timeout):
        super(GogameAppCreate, self).__init__(middleware)
        self.timeout = timeout

    def execute(self, objfile, chiefs=None):
        if self.middleware.is_success(self.taskname):
            return
        appendpoint = self.middleware.reflection()
        # 创建实体
        self.middleware.waiter = appendpoint.create_entity(self.middleware.entity,
                                                           self.middleware.objtype,
                                                           objfile, self.timeout,
                                                           self.middleware.databases, chiefs)

    def revert(self, result, **kwargs):
        super(GogameAppCreate, self).revert(result, **kwargs)
        if isinstance(result, failure.Failure):
            LOG.debug(result.pformat(traceback=True))
            # 外部会自动清理,这里不需要回滚
            self.middleware.set_return(self.taskname, task_common.REVERTED)


def create_entity(appendpoint, entity, objtype, databases,
                  chiefs, objfile, timeout):
    middleware = GogameMiddle(endpoint=appendpoint, entity=entity, objtype=objtype)

    conf = CONF['%s.%s' % (common.NAME, objtype)]
    _database = []
    # format database to class
    for subtype in databases:
        database_id = databases[subtype]
        schema = '%s_%s_%s_%d' % (common.NAME, objtype, subtype, entity)
        # 默认认证
        postfix = '-%d' % entity
        auth = dict(user=conf.get('%s_%s' % (subtype, 'user')) + postfix,
                    passwd=conf.get('%s_%s' % (subtype, 'passwd')),
                    ro_user=conf.get('%s_%s' % (subtype, 'ro_user')) + postfix,
                    ro_passwd=conf.get('%s_%s' % (subtype, 'ro_passwd')),
                    source='%s/%s' % (appendpoint.manager.ipnetwork.network,
                                      appendpoint.manager.ipnetwork.netmask))
        LOG.debug('Create schema %s in %d with auth %s' % (schema, database_id, str(auth)))
        _database.append(GogameCreateDatabase(database_id=database_id, schema=schema,
                                              character_set='utf8',
                                              subtype=subtype,
                                              host=None, port=None, **auth))

    app = application.Application(middleware,
                                  createtask=GogameAppCreate(middleware, timeout),
                                  databases=_database)

    book = LogBook(name='create_%s_%d' % (appendpoint.namespace, entity))
    store = dict(objfile=objfile,  chiefs=chiefs, download_timeout=timeout)
    taskflow_session = sqlite.get_taskflow_session()
    create_flow = pipe.flow_factory(taskflow_session, applications=[app, ], store=store,
                                    db_flow_factory=create_db_flowfactory)
    connection = Connection(taskflow_session)
    engine = load(connection, create_flow, store=store,
                  book=book, engine_cls=ParallelActionEngine)

    try:
        engine.run()
    except Exception as e:
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.exception('Task execute fail')
        else:
            LOG.error('Task execute fail, %s %s' % (e.__class__.__name__, str(e)))
    finally:
        connection.destroy_logbook(book.uuid)
        for dberror in middleware.dberrors:
            LOG.error(str(dberror))
    return middleware
