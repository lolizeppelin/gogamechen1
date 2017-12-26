# -*- coding:utf-8 -*-
from simpleutil.config import cfg
from simpleutil.log import log as logging

from simpleflow.task import Task
from simpleflow.api import load
from simpleflow.types import failure
from simpleflow.storage import Connection
from simpleflow.patterns import unordered_flow as uf
from simpleflow.storage.middleware import LogBook
from simpleflow.engines.engine import ParallelActionEngine

from goperation.manager.rpc.agent import sqlite
from goperation.manager.rpc.agent.application.taskflow.middleware import EntityMiddleware
from goperation.manager.rpc.agent.application.taskflow import application
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


class GogameDatabaseCreate(StandardTask):

    def __init__(self, middleware, data):
        self.data = data
        super(GogameDatabaseCreate, self).__init__(middleware)

    def execute(self):
        # 创建并绑定数据库
        auth = self.data.get('auth')
        dbresult = self.client.schemas_create(self.data.get('database_id'),
                                              body={'schema': self.data.get('schema'),
                                                    'auth': auth,
                                                    'bond': {'entity': self.data.get('entity'),
                                                             'endpoint': common.NAME}})['data'][0]
        # 设置返回结果
        self.middleware.database.setdefault(self.data.get('subtype'), dict(schema=dbresult.get('schema'),
                                                                           database_id=dbresult.get('database_id'),
                                                                           quote_id=dbresult.get('quote_id'),
                                                                           host=dbresult.get('host'),
                                                                           user=auth.get('user'),
                                                                           passwd=auth.get('passwd'),
                                                                           ro_user=auth.get('ro_user'),
                                                                           ro_passwd=auth.get('ro_passwd')
                                                                           ))




    def revert(self, *args, **kwargs):
        result = kwargs.get('result') or args[0]
        super(GogameDatabaseCreate, self).revert(result, **kwargs)
        if isinstance(result, failure.Failure):
            LOG.error('Create schema %s on %d fail' % (self.data.get('schema'),
                                                       self.data.get('database_id')))
            return
        # 弹出返回结果, 解绑并删除
        dbresult = self.middleware.database.pop(self.data.get('subtype'))
        schema = dbresult.get('schema')
        database_id = dbresult.get('database_id')
        unquotes = [dbresult.get('quote_id')]
        try:
            self.middleware.reflection().client.schemas_delete(database_id=database_id,
                                                               schema=schema,
                                                               body={'unquotes': unquotes})
        except Exception as e:
            self.middleware.dberrors.append(dict(database_id=database_id, schema=schema, unquotes=unquotes,
                                                 resone='%s :%s' % (e.__class__.__name__, e.message)))
        else:
            self.middleware.set_return(self.__class__.__name__, task_common.REVERTED)


def create_db_flowfactory(app, store):
    # 数据库创建工作流生成
    middleware = app.middleware
    entity = middleware.entity
    objtype = middleware.objtype
    uflow = uf.Flow('create_%s_%d' % (common.NAME, objtype))
    appendpoint = middleware.reflection()
    for database in app.databases:
        subtype = database.get('subtype')
        database_id = database.get('database_id')
        schema = '%s_%s_%s_%d' % (common.NAME, objtype, subtype, entity)
        conf = CONF['%s.%s' % (common.NAME, objtype)]
        # 默认认证
        auth = dict(user=conf.get('%s_%s' % (subtype, 'user')),
                    passwd=conf.get('%s_%s' % (subtype, 'passwd')),
                    ro_user=conf.get('%s_%s' % (subtype, 'ro_user')),
                    ro_passwd=conf.get('%s_%s' % (subtype, 'ro_passwd')),
                    source='%s/%s' % (appendpoint.ipnetwork.network, appendpoint.ipnetwork.netmask))
        uflow.add(GogameDatabaseCreate(middleware,
                                       dict(database_id=database_id, subtype=subtype,
                                            schema=schema, auth=auth, entity=entity)))
    return uflow


class GogameAppCreate(application.AppCreateBase):

    def __init__(self, middleware, timeout):
        super(GogameAppCreate, self).__init__(middleware)
        self.timeout = timeout

    def execute(self, objfile):
        if self.middleware.is_success(self.__class__.__name__):
            return
        endpoint = self.middleware.reflection()
        # 创建实体
        endpoint.create_entity(self.middleware.entity, self.middleware.objtype, objfile, self.timeout)

    def revert(self, result, **kwargs):
        super(GogameAppCreate, self).revert(result, **kwargs)
        if isinstance(result, failure.Failure):
            LOG.debug(result.pformat(traceback=True))
            endpoint = self.middleware.reflection()
            # 删除实体
            endpoint.delete_entity(self.middleware.entity)
            self.middleware.set_return(self.__class__.__name__, task_common.REVERTED)


class ConfigUpdate(application.AppUpdateBase):

    def execute(self, chiefs=None):
        endpoint = self.middleware.reflection()
        # 调用更新配置
        endpoint.flush_config(self.middleware.entity, self.middleware.databases, chiefs)

    def revert(self, result, **kwargs):
        super(ConfigUpdate, self).revert(result, **kwargs)
        if isinstance(result, failure.Failure):
            LOG.error('%s:%d Update config fail' % (self.middleware.objtype, self.middleware.entity))


def create_entity(appendpoint, entity, objtype, databases,
                  chiefs, objfile, timeout):
    middleware = GogameMiddle(endpoint=appendpoint, entity=entity, objtype=objtype)
    app = application.Application(middleware,
                                  createtask=GogameAppCreate(middleware, timeout),
                                  updatetask=ConfigUpdate(middleware),
                                  databases=databases)

    book = LogBook(name='create_%s_%d' % (appendpoint.namespace, entity))
    store = dict(objfile=objfile,  chiefs=chiefs)
    taskflow_session = sqlite.get_taskflow_session()
    create_flow = pipe.flow_factory(taskflow_session, applications=[app, ],
                                    db_flow_factory=create_db_flowfactory)
    connection = Connection(taskflow_session)
    engine = load(connection, create_flow, store=store,
                  book=book, engine_cls=ParallelActionEngine)

    def wapper():
        try:
            engine.run()
        except Exception:
            LOG.error('create middleware result %s' % str(middleware))
            raise
        finally:
            connection.destroy_logbook(book.uuid)
            for dberror in middleware.dberrors:
                LOG.error(str(dberror))
    return wapper
