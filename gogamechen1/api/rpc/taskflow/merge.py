# -*- coding:utf-8 -*-
import os
import six
import cPickle

from simpleutil.config import cfg
from simpleutil.log import log as logging

from simpleflow.api import load
from simpleflow.types import failure
from simpleflow.storage import Connection
from simpleflow.storage.middleware import LogBook
from simpleflow.engines.engine import ParallelActionEngine

from goperation.manager.rpc.agent import sqlite
from goperation.manager.rpc.agent.application.taskflow import application

from goperation.manager.rpc.agent.application.taskflow.database import MysqlCreate
from goperation.manager.rpc.agent.application.taskflow import pipe
from goperation.taskflow import common as task_common

from gogamechen1 import common
from gogamechen1.api.rpc.taskflow import GogameMiddle
from gogamechen1.api.rpc.taskflow import GogameDatabase
from gogamechen1.api.rpc.taskflow import GogameAppFile

CONF = cfg.CONF

LOG = logging.getLogger(__name__)

SWALLOW = 'SWALLOW'
SWALLOWED = 'SWALLOWED'
DUMPING = 'DUMPING'
INSERT = 'INSERT'


def merge_entitys(endpoint, uuid, entitys, databases):
    mergepath = 'merge-%s' % uuid
    mergeroot = os.path.join(endpoint.endpoint_backup, mergepath)
    if not os.path.exists(mergeroot):
        os.makedirs(mergeroot)
    stepsfile = os.path.join(mergeroot, 'steps.dat')
    if not os.path.exists(stepsfile):
        steps = {}
        for entity in entitys:
            steps[entity] = SWALLOW
        with open(stepsfile, 'wb') as f:
            cPickle.dump(steps, f)
    steps = cPickle.load(stepsfile)
    if set(steps.keys()) != set(entitys):
        raise
    prepares = []
    for entity, step in six.iteritems(steps):
        if step != INSERT:
            prepares.append(entity)
    if prepares:
        pass
