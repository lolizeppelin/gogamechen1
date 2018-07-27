# -*- coding:utf-8 -*-
import os
import time
import six
import eventlet
import cPickle
import contextlib

import mysql
import mysql.connector

from simpleutil.config import cfg
from simpleutil.log import log as logging
from simpleutil.utils import systemutils
from simpleutil.utils.systemutils import posix
from simpleutil.utils.systemutils import ExitBySIG
from simpleutil.utils.systemutils import UnExceptExit

from simpleservice.ormdb.tools.backup import mysqldump
from simpleservice.ormdb.tools.backup import mysqlload

from simpleflow.utils.storage_utils import build_session
from simpleflow.api import load
from simpleflow.task import Task
from simpleflow.types import failure
from simpleflow.patterns import linear_flow as lf
from simpleflow.patterns import unordered_flow as uf
from goperation.manager.rpc.agent import sqlite
from simpleflow.storage.middleware import LogBook
from simpleflow.storage import Connection
from simpleflow.engines.engine import ParallelActionEngine

from goperation.utils import safe_fork
from goperation.manager import common as manager_common

from gogamechen1 import common
from gogamechen1.api import exceptions

CONF = cfg.CONF

LOG = logging.getLogger(__name__)

SWALLOW = 'SWALLOW'
DUMPING = 'DUMPING'
SWALLOWED = 'SWALLOWED'
INSERT = 'INSERT'
FINISHED = 'FINISHED'


def sqlfile(entity):
    return '%s-db-%d.sql' % (common.GAMESERVER, entity)


@contextlib.contextmanager
def dbconnect(host, port, user, passwd, schema,
              raise_on_warnings=True):
    if not schema:
        raise ValueError('Schema is none?')
    kwargs = dict(user=user, passwd=passwd,
                  host=host, port=port,
                  database=schema,
                  raise_on_warnings=raise_on_warnings)
    conn = mysql.connector.connect(**kwargs)
    try:
        yield conn
    finally:
        conn.close()


def cleandb(host, port, user, passwd, schema):
    """drop 所有表"""
    with dbconnect(host=host, port=port,
                   user=user, passwd=passwd,
                   schema=schema) as conn:
        cursor = conn.cursor()
        cursor.execute('show tables')
        talbes = cursor.fetchall()
        for table in talbes:
            cursor.execute('drop table %s' % table[0])
            cursor.fetchall()
        cursor.close()


class Swallow(Task):

    def __init__(self, uuid, steps, entity, endpoint):
        self.endpoint = endpoint
        self.entity = entity
        self.stpes = steps
        self.uuid = uuid
        super(Swallow, self).__init__(name='swallow_%d' % entity, provides='db_%d' % entity)

    def execute(self, entity, timeout):
        step = self.stpes[self.entity]
        if step in (DUMPING, SWALLOW):
            with self.endpoint.mlock:
                result = self.endpoint.client.swallow_entity(self.entity, self.uuid, entity)
            if result.get('resultcode') != manager_common.RESULT_SUCCESS or not result.get('data'):
                LOG.error('Swallow success, but can not find database from result')
                return None
            data = result.get('data')
            databases = data[0].get('databases')
            if not databases:
                LOG.error('Swallow success, databases is empty')
                return None
            self.stpes[self.entity] = DUMPING
            return databases
        return None


class DumpData(Task):

    NODUMPTABLES = [
        'mining_area',
        'timer_boss',
        'pvp_arena_rank',
        'pvp_arena_pet_rank',
        'var_world'
    ]

    def __init__(self, uuid, steps, entity,
                 endpoint=None):
        self.entity = entity
        self.stpes = steps
        self.uuid = uuid
        self.endpoint = endpoint
        super(DumpData, self).__init__(name='dump_%d' % entity,
                                       rebind=['mergeroot', 'dtimeout', 'db_%d' % entity])

    @staticmethod
    def _ext_args(schema):
        extargs = ['-t']
        for table in DumpData.NODUMPTABLES:
            extargs.append('--ignore-table=%s.%s' % (schema, table))
        return extargs

    @staticmethod
    def _prepare_database(databases):
        return databases[common.DATADB]

    def execute(self, root, timeout, databases):
        """
        导出需要合并的实体数据库
        如果init.sql文件不存在,导出一份init.sql文件
        """
        step = self.stpes[self.entity]
        if step == DUMPING:
            _file = os.path.join(root, sqlfile(self.entity))
            if os.path.exists(_file):
                return
            database = DumpData._prepare_database(databases)
            try:
                mysqldump(_file,
                          database.get('host'), database.get('port'),
                          database.get('user'), database.get('passwd'),
                          database.get('schema'),
                          character_set=None,
                          extargs=self._ext_args(database.get('schema')),
                          logfile=None, callable=safe_fork,
                          timeout=timeout)
            except (ExitBySIG, UnExceptExit):
                LOG.error('Dump from mysql fail')
                if os.path.exists(_file):
                    try:
                        os.remove(_file)
                    except (OSError, OSError):
                        LOG.error('Try remove file %d fail!' % _file)
                        raise exceptions.MergeException('Remove error file %s fail' % _file)
            else:
                self.stpes[self.entity] = SWALLOWED
            # create init file
            initfile = os.path.join(root, 'init.sql')
            if not os.path.exists(initfile):
                try:
                    with self.endpoint.mlock:
                        if not os.path.exists(initfile):
                            LOG.info('Dump init sql from entity %d, schema %s' % (self.entity, database.get('schema')))
                            mysqldump(initfile,
                                      database.get('host'), database.get('port'),
                                      database.get('user'), database.get('passwd'),
                                      database.get('schema'),
                                      character_set=None, extargs=['-R', '-d'],
                                      logfile=None, callable=safe_fork,
                                      timeout=timeout)
                except (ExitBySIG, UnExceptExit):
                    if os.path.exists(initfile):
                        try:
                            os.remove(initfile)
                        except (OSError, OSError):
                            LOG.error('Try remove init sql file fail!')


class Swallowed(Task):

    def __init__(self, uuid, steps, entity, endpoint):
        self.endpoint = endpoint
        self.entity = entity
        self.stpes = steps
        self.uuid = uuid
        super(Swallowed, self).__init__(name='swallowed_%d' % entity)

    def execute(self, entity, timeout):
        step = self.stpes[self.entity]
        if step == SWALLOWED:
            with self.endpoint.mlock:
                result = self.endpoint.client.swallowed_entity(self.entity, self.uuid, entity)
            try:
                if result.get('resultcode') != manager_common.RESULT_SUCCESS or not result.get('data'):
                    LOG.error('Swallowed success, but can not find areas from result')
                    return None
                data = result.get('data')
                areas = data[0].get('areas')
                if not areas:
                    raise KeyError('Not areas found')
            except KeyError as e:
                LOG.error('Get areas fail %s' % e.message)
            else:
                self.stpes[self.entity] = INSERT
                if entity not in self.endpoint.konwn_appentitys:
                    eventlet.sleep(3)
                self.endpoint.konwn_appentitys[entity]['areas'].extend(areas)
                LOG.info('Extend new areas of konwn appentitys success')


class SafeCleanDb(Task):

    def __init__(self):
        super(SafeCleanDb, self).__init__(name='cleandb')

    def execute(self, root, database):
        """清空前备份数据库,正常情况下备份内容为空"""
        safebak = os.path.join(root, 'safebak.%d.gz' % time.time())
        # back up database
        mysqlload(safebak,
                  database.get('host'), database.get('port'),
                  database.get('user'), database.get('passwd'),
                  database.get('schema'),
                  character_set=None, extargs=['-R'],
                  logfile=None, callable=safe_fork,
                  timeout=15)
        # drop all table
        cleandb(host=database.get('host'), port=database.get('port'),
                user=database.get('user'), passwd=database.get('passwd'),
                schema=database.get('schema'))


class InitDb(Task):
    def __init__(self):
        super(InitDb, self).__init__(name='initdb')

    @staticmethod
    def _predo(root, database):
        """对原始数据库做特殊处理"""
        prefile = os.path.join(root, 'pre.sql')
        if os.path.exists(prefile):
            mysqlload(prefile,
                      database.get('host'), database.get('port'),
                      database.get('user'), database.get('passwd'),
                      database.get('schema'),
                      character_set=None, extargs=None,
                      logfile=None, callable=safe_fork,
                      timeout=30)

    def execute(self, root, database):
        LOG.info('Try init databases')
        initfile = os.path.join(root, 'init.sql')
        mysqlload(initfile,
                  database.get('host'), database.get('port'),
                  database.get('user'), database.get('passwd'),
                  database.get('schema'),
                  character_set=None, extargs=None,
                  logfile=None, callable=safe_fork,
                  timeout=15)
        self._predo(root, database)


class InserDb(Task):
    """插入各个实体的数据库"""

    def __init__(self, entity):
        super(InserDb, self).__init__(name='insert-%d' % entity)

    def execute(self, root, database):
        LOG.info('Insert database of entity %d' % self.entity)
        _file = os.path.join(root, sqlfile(self.entity))
        mysqlload(_file,
                  database.get('host'), database.get('port'),
                  database.get('user'), database.get('passwd'),
                  database.get('schema'),
                  character_set=None, extargs=None,
                  logfile=None, callable=safe_fork,
                  timeout=30)

    def revert(self, result, database, **kwargs):
        """插入失败清空数据库"""
        if isinstance(result, failure.Failure):
            LOG.warning('Insert database of entity %d fail, try clean database' % self.entity)
            cleandb(host=database.get('host'), port=database.get('port'),
                    user=database.get('user'), passwd=database.get('passwd'),
                    schema=database.get('schema'))


class PostDo(Task):
    def __init__(self, uuid, endpoint):
        self.uuid = uuid
        self.endpoint = endpoint
        super(PostDo, self).__init__(name='postdo')

    @staticmethod
    def _postdo(root, database):
        """合并完成后特殊处理"""
        postfile = os.path.join(root, 'post.sql')
        if os.path.exists(postfile):
            mysqlload(postfile,
                      database.get('host'), database.get('port'),
                      database.get('user'), database.get('passwd'),
                      database.get('schema'),
                      character_set=None, extargs=None,
                      logfile=None, callable=safe_fork,
                      timeout=30)

    def execute(self, root, database):
        """post execute"""
        try:
            self._postdo(root, database)
        except Exception:
            LOG.exception('Post databse execute fail')
        else:
            # 通知合服完毕
            self.endpoint.client.swallowe_finish(self.uuid)


def create_merge(appendpoint, uuid, entitys, middleware, opentime, chiefs):
    mergepath = 'merge-%s' % uuid
    mergeroot = os.path.join(appendpoint.endpoint_backup, mergepath)
    if not os.path.exists(mergeroot):
        os.makedirs(mergeroot)
    stepsfile = os.path.join(mergeroot, 'steps.dat')
    if os.path.exists(stepsfile):
        raise exceptions.MergeException('Steps file exist, can not merge')
    data = {}
    steps = {}
    for _entity in entitys:
        steps[_entity] = SWALLOW
    data['opentime'] = opentime
    data['chiefs'] = chiefs
    data['steps'] = steps
    with open(stepsfile, 'wb') as f:
        cPickle.dump(data, f)
    merge_entitys(appendpoint, uuid, middleware.entity, middleware.databases)


def merge_entitys(appendpoint, uuid, entity, databases):
    datadb = databases[common.DATADB]
    mergepath = 'merge-%s' % uuid
    mergeroot = os.path.join(appendpoint.endpoint_backup, mergepath)
    stepsfile = os.path.join(mergeroot, 'steps.dat')
    initfile = os.path.join(mergeroot, 'init.sql')
    if not os.path.exists(stepsfile):
        raise exceptions.MergeException('Steps file not exist')
    with open(stepsfile, 'rb') as f:
        data = cPickle.load(f)
        steps = data['steps']
    prepares = []
    for _entity, step in six.iteritems(steps):
        if step == FINISHED:
            raise exceptions.MergeException('Steps is finish?')
        if step != INSERT:
            prepares.append(_entity)
    if prepares:
        name = 'prepare-merge-at-%d' % int(time.time())
        book = LogBook(name=name)
        store = dict(timeout=5, dtimeout=60, mergeroot=mergeroot, entity=entity)
        taskflow_session = build_session('sqlite:///%s' % os.path.join(mergeroot, '%s.db' % name))
        connection = Connection(taskflow_session)

        prepare_uflow = uf.Flow(name)
        for _entity in prepares:
            entity_flow = lf.Flow('prepare-%d' % _entity)
            entity_flow.add(Swallow(uuid, steps, _entity, appendpoint))
            entity_flow.add(DumpData(uuid, steps, _entity, appendpoint))
            entity_flow.add(Swallowed(uuid, steps, _entity, appendpoint))
            prepare_uflow.add(entity_flow)
        engine = load(connection, prepare_uflow, store=store,
                      book=book, engine_cls=ParallelActionEngine,
                      max_workers=4)
        try:
            engine.run()
        except Exception as e:
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.exception('Prepare merge task execute fail')
            else:
                LOG.error('Prepare merge task execute fail, %s %s' % (e.__class__.__name__, str(e)))
            raise
        finally:
            connection.session = None
            taskflow_session.close()
            with open(stepsfile, 'wb') as f:
                cPickle.dump(data, f)

    for _entity, step in six.iteritems(steps):
        if step != INSERT:
            raise exceptions.MergeException('Some step not on %s' % INSERT)
        if not os.path.exists(os.path.join(mergeroot, sqlfile(_entity))):
            raise exceptions.MergeException('Entity %d sql file not exist' % _entity)

    if not os.path.exists(initfile):
        LOG.error('Init database file not exist')
        raise exceptions.MergeException('Init database file not exist')
    LOG.info('Prepare merge success, try insert datadatabase')

    name = 'merge-at-%d' % int(time.time())
    book = LogBook(name=name)
    store = dict(timeout=5, mergeroot=mergeroot, database=datadb)
    taskflow_session = build_session('sqlite:///%s' % os.path.join(mergeroot, '%s.db' % name))
    connection = Connection(taskflow_session)

    merge_flow = lf.Flow('merge-to')
    merge_flow.add(InitDb())
    merge_flow.add(SafeCleanDb())
    insert_uflow = uf.Flow('insert-db')
    for _entity in steps:
        insert_uflow.add(InserDb(_entity))
    merge_flow.add(insert_uflow)
    merge_flow.add(PostDo(uuid, appendpoint))

    engine = load(connection, merge_flow, store=store,
                  book=book, engine_cls=ParallelActionEngine,
                  max_workers=4)
    try:
        engine.run()
    except Exception as e:
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.exception('Merge database task execute fail')
        else:
            LOG.error('Merge database task execute fail, %s %s' % (e.__class__.__name__, str(e)))
    else:
        for _entity in steps:
            steps[_entity] = FINISHED
        with open(stepsfile, 'wb') as f:
            cPickle.dump(data, f)
        appendpoint.client.finish_merge(uuid)
    finally:
        connection.session = None
        taskflow_session.close()
        appendpoint.flush_config(entity, databases,
                                 opentime=data['opentime'],
                                 chiefs=data['chiefs'])
