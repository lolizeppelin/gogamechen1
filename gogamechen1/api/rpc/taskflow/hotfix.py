# -*- coding:utf-8 -*-
import os
from simpleutil.config import cfg
from simpleutil.log import log as logging
from simpleutil.utils.zlibutils.excluder import Excluder

from simpleflow.api import load
from simpleflow.storage import Connection
from simpleflow.storage.middleware import LogBook
from simpleflow.engines.engine import ParallelActionEngine

from goperation.manager.rpc.agent import sqlite
from goperation.manager.rpc.agent.application.taskflow.application import AppFileUpgradeByFile
from goperation.manager.rpc.agent.application.taskflow.application import Application
from goperation.manager.rpc.agent.application.taskflow import pipe
from gogamechen1 import common
from gogamechen1.api.rpc.taskflow import GogameMiddle
from gogamechen1.api.rpc.taskflow import GogameAppFile
from gogamechen1.api.rpc.taskflow import GogameAppBackupFile

CONF = cfg.CONF

LOG = logging.getLogger(__name__)

SHELLZIPEXCLUDES = ['"bin**"', '"geology**"']
SHELTAREXCLUDE = ['bin', 'geology']


class HOFIXExcluder(Excluder):

    def __call__(self, compretype, shell=False):
        """find excluder function"""
        if not shell:
            raise TypeError('Just for shell extract')
        if compretype == 'zip':
            return self.unzip
        elif compretype == 'gz':
            return self.untar
        else:
            raise NotImplementedError('Can not extract %s file' % compretype)

    @staticmethod
    def unzip():
        return SHELLZIPEXCLUDES

    @staticmethod
    def untar():
        return SHELTAREXCLUDE


hofixexcluer = HOFIXExcluder()


def hotfix_entitys(appendpoint,
                   objtype, appfile,
                   entitys, timeline):
    backupfile = None
    download_time = 600
    upzip_timeout = 600
    md5 = appfile.get('md5')
    backup = appfile.get('backup', True)
    revertable = appfile.get('revertable', False)
    rollback = appfile.get('rollback', True)
    timeout = appfile.get('timeout')
    if timeout < download_time:
        download_time = timeout
    if timeout < upzip_timeout:
        upzip_timeout = timeout
    # 程序更新文件
    upgradefile = GogameAppFile(md5, objtype, rollback=rollback, revertable=revertable)
    if backup:
        # 备份entity在flow_factory随机抽取
        outfile = os.path.join(appendpoint.endpoint_backup,
                               '%s.%s.%d.gz' % (objtype, common.APPFILE, timeline))
        # 程序备份文件
        backupfile = GogameAppBackupFile(outfile, objtype)

    applications = []
    middlewares = []
    _updates = {}
    for entity in entitys:
        if objtype != appendpoint._objtype(entity):
            raise ValueError('Entity not the same objtype')
        middleware = GogameMiddle(endpoint=appendpoint, entity=entity, objtype=objtype)
        middlewares.append(middleware)
        _updates.clear()
        upgradetask = AppFileUpgradeByFile(middleware, exclude=hofixexcluer,
                                           rebind=['upgradefile', 'upzip_timeout'])
        app = Application(middleware, upgradetask=upgradetask)
        applications.append(app)

    book = LogBook(name='hotfix_%s' % appendpoint.namespace)
    store = dict(download_timeout=download_time, upzip_timeout=upzip_timeout)
    taskflow_session = sqlite.get_taskflow_session()
    upgrade_flow = pipe.flow_factory(taskflow_session, book,
                                     applications=applications,
                                     upgradefile=upgradefile,
                                     backupfile=backupfile,
                                     store=store)
    connection = Connection(taskflow_session)
    engine = load(connection, upgrade_flow, store=store,
                  book=book, engine_cls=ParallelActionEngine,
                  max_workers=4)
    e = None
    try:
        engine.run()
    except Exception as e:
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.exception('Hotfix task execute fail')
        else:
            LOG.error('Hotfix task execute fail, %s %s' % (e.__class__.__name__, str(e)))
    finally:
        connection.destroy_logbook(book.uuid)
    return middlewares, e
