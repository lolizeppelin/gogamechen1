import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext import declarative

from simpleutil.utils import uuidutils

from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.dialects.mysql import SMALLINT
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.dialects.mysql import CHAR

from simpleservice.ormdb.models import TableBase
from simpleservice.ormdb.models import InnoDBTableBase
from simpleservice.ormdb.models import MyISAMTableBase

from gogamechen1 import common

TableBase = declarative.declarative_base(cls=TableBase)


class ObjtypeFile(TableBase):
    uuid = sa.Column(CHAR(36), default=uuidutils.generate_uuid,
                     nullable=False, primary_key=True)
    objtype = sa.Column(VARCHAR(64), nullable=False)
    subtype = sa.Column(VARCHAR(64), nullable=False)
    version = sa.Column(VARCHAR(64), nullable=False)

    __table_args__ = (
        sa.UniqueConstraint('objtype', 'subtype', 'version', name='file_unique'),
        MyISAMTableBase.__table_args__
    )


class AreaDatabase(TableBase):
    quote_id = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True)
    database_id = sa.Column(INTEGER(unsigned=True), nullable=False)
    entity = sa.Column(sa.ForeignKey('appentitys.entity', ondelete="CASCADE", onupdate='RESTRICT'),
                       nullable=False)
    subtype = sa.Column(VARCHAR(64), nullable=False)
    host = sa.Column(VARCHAR(200), default=None, nullable=False)
    port = sa.Column(SMALLINT(unsigned=True), default=3306, nullable=False)
    user = sa.Column(VARCHAR(64), default=None, nullable=False)
    passwd = sa.Column(VARCHAR(128), default=None, nullable=False)
    ro_user = sa.Column(VARCHAR(64), default=None, nullable=False)
    ro_passwd = sa.Column(VARCHAR(128), default=None, nullable=False)
    character_set = sa.Column(VARCHAR(64), default='utf8', nullable=True)

    __table_args__ = (
        sa.UniqueConstraint('entity', 'subtype', name='type_unique'),
        InnoDBTableBase.__table_args__
    )


class GameArea(TableBase):
    area_id = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True)
    group_id = sa.Column(sa.ForeignKey('groups.group_id', ondelete="RESTRICT", onupdate='RESTRICT'),
                         nullable=False, primary_key=True)
    entity = sa.Column(sa.ForeignKey('appentitys.entity', ondelete="RESTRICT", onupdate='RESTRICT'),
                       nullable=False)

    __table_args__ = (
        sa.Index('group_index', 'group_id'),
        InnoDBTableBase.__table_args__
    )


class AppEntity(TableBase):
    entity = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True)
    agent_id = sa.Column(INTEGER(unsigned=True), nullable=False)
    group_id = sa.Column(sa.ForeignKey('groups.group_id', ondelete="RESTRICT", onupdate='RESTRICT'),
                         nullable=False)
    objtype = sa.Column(VARCHAR(64), nullable=False)
    opentime = sa.Column(INTEGER(unsigned=True), nullable=True)
    status = sa.Column(TINYINT(64), nullable=False, default=common.UNACTIVE)
    cross_id = sa.Column(INTEGER(unsigned=True), nullable=True)
    areas = orm.relationship(GameArea, backref='appentity', lazy='select',
                             cascade='delete,delete-orphan')
    databases = orm.relationship(AreaDatabase, backref='appentity', lazy='select',
                                 cascade='delete,delete-orphan')

    __table_args__ = (
        sa.Index('agent_id_index', 'agent_id'),
        sa.Index('group_id_index', 'group_id'),
        InnoDBTableBase.__table_args__
    )


class Group(TableBase):
    group_id = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True,
                         autoincrement=True)
    name = sa.Column(VARCHAR(64), default=None, nullable=False)
    lastarea = sa.Column(INTEGER(unsigned=True), nullable=False, default=0)
    desc = sa.Column(VARCHAR(256), nullable=True)
    areas = orm.relationship(GameArea, backref='group', lazy='select',
                                 cascade='delete,delete-orphan')
    entitys = orm.relationship(AppEntity, backref='group', lazy='select',
                               cascade='delete,delete-orphan')

    __table_args__ = (
        sa.UniqueConstraint('name', name='group_unique'),
        InnoDBTableBase.__table_args__
    )
