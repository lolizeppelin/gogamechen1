# -*- coding:utf-8 -*-
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext import declarative

from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.dialects.mysql import SMALLINT
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.dialects.mysql import ENUM
from sqlalchemy.dialects.mysql import BLOB

from simpleservice.ormdb.models import TableBase
from simpleservice.ormdb.models import InnoDBTableBase

from goperation.manager import common as manager_common

from gogamechen1 import common

TableBase = declarative.declarative_base(cls=TableBase)


class PackageRemark(TableBase):
    remark_id = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True, autoincrement=True)
    package_id = sa.Column(sa.ForeignKey('packages.package_id', ondelete="CASCADE", onupdate='RESTRICT'),
                           nullable=False)
    rtime = sa.Column(INTEGER(unsigned=True), nullable=False)
    username = sa.Column(VARCHAR(64), nullable=False)
    message = sa.Column(VARCHAR(512), nullable=False)


class PackageFile(TableBase):
    # 包文件id
    pfile_id = sa.Column(INTEGER(unsigned=True), nullable=False,
                         primary_key=True, autoincrement=True)
    # 安装包引用的resource_id, None则为外部地址
    resource_id = sa.Column(INTEGER(unsigned=True), nullable=True)
    # 安装包文件名
    filename = sa.Column(VARCHAR(128), nullable=True)
    package_id = sa.Column(sa.ForeignKey('packages.package_id', ondelete="RESTRICT", onupdate='RESTRICT'),
                           nullable=False)
    # 包类型
    ftype = sa.Column(VARCHAR(32), nullable=False)
    # 安装包版本号
    gversion = sa.Column(VARCHAR(64), nullable=False)
    address = sa.Column(VARCHAR(200), nullable=True)
    uptime = sa.Column(INTEGER(unsigned=True), nullable=False)
    status = sa.Column(VARCHAR(16), ENUM(*manager_common.DOWNFILESTATUS),
                       default=manager_common.DOWNFILE_FILEOK, nullable=False)
    desc = sa.Column(VARCHAR(256), nullable=True)
    __table_args__ = (
        sa.UniqueConstraint('address', name='address_unique'),
        sa.Index('ftype_index', 'ftype'),
        InnoDBTableBase.__table_args__
    )


class Package(TableBase):
    package_id = sa.Column(INTEGER(unsigned=True), nullable=False,
                           primary_key=True, autoincrement=True)
    # 游戏资源引用id, 游戏下载资源引用的resource id
    resource_id = sa.Column(INTEGER(unsigned=True), nullable=False)
    # 游戏资源默认版本
    rversion = sa.Column(VARCHAR(64), nullable=True)
    # 默认版本引用id
    rquote_id = sa.Column(INTEGER(unsigned=True), nullable=True)
    # 包名,一般情况下唯一
    package_name = sa.Column(VARCHAR(64), nullable=False)
    # 安装包版本号,  对应PackageFile中的pfile_id
    # gversion = sa.Column(VARCHAR(64), nullable=True)
    gversion = sa.Column(INTEGER(unsigned=True), nullable=True)
    # 游戏服务器组id
    group_id = sa.Column(sa.ForeignKey('groups.group_id', ondelete="RESTRICT", onupdate='RESTRICT'),
                         nullable=False)
    # 标记
    mark = sa.Column(VARCHAR(32), nullable=False)
    # 状态
    status = sa.Column(SMALLINT, nullable=False, default=common.ENABLE)
    # 说明
    desc = sa.Column(VARCHAR(256), nullable=True)
    # 特殊标记
    magic = sa.Column(BLOB, nullable=True)
    # 扩展字段
    extension = sa.Column(BLOB, nullable=True)
    files = orm.relationship(PackageFile, backref='package', lazy='select',
                             cascade='delete,delete-orphan,save-update')
    __table_args__ = (
        sa.UniqueConstraint('package_name', name='package_unique'),
        InnoDBTableBase.__table_args__
    )


class ObjtypeFile(TableBase):
    md5 = sa.Column(CHAR(36), nullable=False, primary_key=True)
    objtype = sa.Column(VARCHAR(64), nullable=False)
    subtype = sa.Column(VARCHAR(64), nullable=False)
    version = sa.Column(VARCHAR(128), nullable=False)
    # cdn资源id,为None表示外部资源
    resource_id = sa.Column(INTEGER(unsigned=True), nullable=True)

    __table_args__ = (
        sa.UniqueConstraint('objtype', 'subtype', 'version', name='file_unique'),
        InnoDBTableBase.__table_args__
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
    area_id = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True,
                        autoincrement=True)
    group_id = sa.Column(sa.ForeignKey('groups.group_id', ondelete="RESTRICT", onupdate='RESTRICT'),
                         nullable=False)
    show_id = sa.Column(INTEGER(unsigned=True), nullable=False)
    areaname = sa.Column(VARCHAR(128), nullable=False)
    entity = sa.Column(sa.ForeignKey('appentitys.entity', ondelete="RESTRICT", onupdate='RESTRICT'),
                       nullable=False)

    __table_args__ = (
        sa.UniqueConstraint('group_id', 'areaname', name='name_unique'),
        sa.Index('group_index', 'group_id'),
        InnoDBTableBase.__table_args__
    )


class AppEntity(TableBase):
    entity = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True)
    agent_id = sa.Column(INTEGER(unsigned=True), nullable=False)
    group_id = sa.Column(sa.ForeignKey('groups.group_id', ondelete="RESTRICT", onupdate='RESTRICT'),
                         nullable=False)
    objtype = sa.Column(VARCHAR(64), nullable=False)
    platform = sa.Column(VARCHAR(64), nullable=True)
    opentime = sa.Column(INTEGER(unsigned=True), nullable=True)
    status = sa.Column(TINYINT(64), nullable=False, default=common.UNACTIVE)
    cross_id = sa.Column(INTEGER(unsigned=True), nullable=True)
    # 资源版本优先级设置, key pakcage id  value  version
    versions = sa.Column(BLOB, nullable=True)
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
    desc = sa.Column(VARCHAR(256), nullable=True)
    areas = orm.relationship(GameArea, backref='group', lazy='select',
                             cascade='delete,delete-orphan')
    entitys = orm.relationship(AppEntity, backref='group', lazy='select',
                               cascade='delete,delete-orphan')
    packages = orm.relationship(Package, backref='group', lazy='select',
                                cascade='delete,delete-orphan')
    __table_args__ = (
        sa.UniqueConstraint('name', name='group_unique'),
        InnoDBTableBase.__table_args__
    )


class MergeEntity(TableBase):
    entity = sa.Column(INTEGER(unsigned=True), nullable=False, primary_key=True)
    status = sa.Column(TINYINT(64), nullable=False, default=common.MERGEING)
    uuid = sa.Column(sa.ForeignKey('mergetasks.uuid', ondelete="RESTRICT", onupdate='RESTRICT'),
                     nullable=False)
    areas = sa.Column(BLOB, nullable=True)
    databases = sa.Column(BLOB, nullable=True)


class MergeTask(TableBase):
    uuid = sa.Column(VARCHAR(36), nullable=False, primary_key=True)
    entity = sa.Column(INTEGER(unsigned=True), default=0)
    status = sa.Column(TINYINT(64), nullable=False, default=common.MERGEING)
    mergetime = sa.Column(INTEGER(unsigned=True), nullable=False)
    entitys = orm.relationship(MergeEntity, backref='mergetask', lazy='select',
                               cascade='delete,delete-orphan')
