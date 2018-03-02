# -*- coding:utf-8 -*-
import os
import time
from simpleutil.config import cfg
from simpleutil.log import log as logging

from simpleflow.api import load
from simpleflow.storage import Connection
from simpleflow.storage.middleware import LogBook
from simpleflow.engines.engine import ParallelActionEngine

from goperation.manager.rpc.agent import sqlite
from goperation.manager.rpc.agent.application.taskflow import application
from goperation.manager.rpc.agent.application.taskflow.database import DbUpdateFile
from goperation.manager.rpc.agent.application.taskflow import pipe
from gogamechen1 import common
from gogamechen1.api.rpc.taskflow import GogameMiddle
from gogamechen1.api.rpc.taskflow import GogameDatabase


CONF = cfg.CONF

LOG = logging.getLogger(__name__)


class GogameStop(application.AppStopBase):
    pass


def upgrade_entitys(appendpoint, entitys, objfiles, objtype):
    _objfiles = {}

    # 下载数据库更新文件任务
    for subtype in (common.DATADB, common.LOGDB):
        if subtype in objfiles:
            upinfo = objfiles[subtype]
            backup = upinfo.get('backup', False)
            rollback = upinfo.get('rollback', False)
            if rollback and not backup:
                raise ValueError('%s rollback need backup')
            _objfiles[subtype] = dict(update=DbUpdateFile(source=upinfo['uuid'],
                                                          rollback=rollback,
                                                          formater=None),
                                      timeout=upinfo['timeout'],
                                      backup=backup)
    backupfile = None
    rollback = None
    if common.APPFILE in objfiles:
        # 下载程序更新文件任务
        upinfo = objfiles[common.APPFILE]
        # TODO 程序文件校验函数,程序文件备份过滤函数
        _objfiles[common.APPFILE] = application.AppUpgradeFile(source=upinfo['uuid'])
        if upinfo.get('backup', True):
            backupfile = os.path.join(appendpoint.endpoint_backup,
                                      '%s.%s.%d.gz' % (objtype, common.APPFILE, int(time.time())))
        rollback = upinfo.get('rollback', False)
        if rollback and not backupfile:
            raise ValueError('%s rollback need backupfile')

    applications = []
    middlewares = []
    for entity in entitys:
        if objtype != appendpoint._objtype(entity):
            raise ValueError('Entity not the same objtype')
        middleware = GogameMiddle(endpoint=appendpoint, entity=entity, objtype=objtype)
        middlewares.append(middleware)
        _database = []
        # 备份数据库信息
        for subtype in (common.DATADB, common.LOGDB):
            if subtype in _objfiles:
                dbinfo = appendpoint.local_database_info(entity, subtype)
                update = _objfiles[subtype]['update']
                timeout = _objfiles[subtype]['timeout']
                backup = None
                if _objfiles[subtype]['backup']:
                    backup = os.path.join(appendpoint.bakpath(entity), '%s.%s.%d.sql' % (objtype, subtype,
                                                                                         int(time.time())))
                    if os.path.exists(backup):
                        raise ValueError('%s backup file exist' % subtype)
                _database.append(GogameDatabase(backup=backup, update=update,
                                                timeout=timeout, **dbinfo))
        # 备份程序文件任务
        upgradetask = None
        if _objfiles.get(common.APPFILE):
            upgradetask = application.AppFileUpgradeByFile(middleware, rollback=rollback)
        app = application.Application(middleware,
                                      upgradetask=upgradetask,
                                      databases=_database)
        applications.append(app)

    book = LogBook(name='upgrad_%s' % appendpoint.namespace)
    # store = dict(objfile=objfile, chiefs=chiefs, download_timeout=timeout)
    store = dict(download_timeout=60, db_dump_timeout=600)
    taskflow_session = sqlite.get_taskflow_session()
    upgrade_flow = pipe.flow_factory(taskflow_session,
                                     applications=applications,
                                     upgradefile=_objfiles.get(common.APPFILE),
                                     backupfile=backupfile,
                                     store=store)
    connection = Connection(taskflow_session)
    engine = load(connection, upgrade_flow, store=store,
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
    return middlewares
