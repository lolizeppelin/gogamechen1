# -*- coding:utf-8 -*-
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


def merge_entitys(*args, **kwargs):
    raise NotImplementedError
