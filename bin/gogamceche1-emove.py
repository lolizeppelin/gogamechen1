#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
数据库实例迁移工具
"""
import logging
import sys

from simpleutil.config import cfg
from simpleutil.config import types
from simpleutil.utils import table

from goperation.api.client.config import client_opts
from goperation.api.client.config import index_opts
from goperation.api.client import ManagerClient

from goperation.api.client.utils import prepare_results

from gopdb.api.client import GopDBClient

CONF = cfg.CONF

create_base_opts = [
    cfg.StrOpt('character_set',
               help='Database schema character set'),
    cfg.StrOpt('collation_type',
               help='Database schema collation type'),
]

auth_opts = [
    cfg.StrOpt('user',
               required=True,
               help='Database schema rw user',
               ),
    cfg.StrOpt('passwd',
               required=True,
               help='Database schema rw user passwd',
               ),
    cfg.StrOpt('ro_user',
               required=True,
               help='Database schema ro user',
               ),
    cfg.StrOpt('ro_passwd',
               required=True,
               help='Database schema ro user passwd',
               ),
    cfg.StrOpt('source',
               help='Database schema visit source limit',
               ),
]

one_opts = [
    cfg.IntOpt('database_id',
               short='d',
               required=True,
               help='Target Database id'),
    cfg.StrOpt('schema',
               required=True,
               help='Database schema name',
               ),
]

del_opts = [
    cfg.ListOpt('unquotes',
                short='q',
                item_type=types.Integer(),
                help='Delete schema with quotes(delete quotes before delete schema)')
]



def client(session=None):
    return GopDBClient(httpclient=ManagerClient(url=CONF.gcenter, port=CONF.gcenter_port,
                                                retries=CONF.retries, timeout=CONF.apitimeout,
                                                token=CONF.trusted, session=session))


def create():
    CONF.register_cli_opts(one_opts)
    CONF.register_cli_opts(create_base_opts)
    CONF.register_cli_opts(auth_opts)
    CONF(project='cmd')

    _client = client()

    body = dict(database_id=CONF.database_id,
                schema=CONF.schema)
    auth = dict(user=CONF.user, passwd=CONF.passwd,
                ro_user=CONF.ro_user, ro_passwd=CONF.ro_passwd)
    if CONF.source:
        auth.setdefault('source', CONF.source)
    body.setdefault('auth', auth)
    if CONF.character_set or CONF.collation_type:
        options = dict()
        if CONF.character_set:
            options.setdefault('character_set', CONF.character_set)
        if CONF.collation_type:
            options.setdefault('collation_type', CONF.collation_type)
        body.setdefault('options', options)

    code, result, data = prepare_results(_client.schemas_create, CONF.database_id, body=body)
    if code:
        print('\033[1;31;40m')
        print 'Fail, code %d, result %s' % (code, result)
        if data:
            print data
        print('\033[0m')
        sys.exit(1)
    schema = data[0]
    print('\033[1;32;40m')
    print 'Create database schema success'
    print 'database id: %d' % schema.get('database_id')
    print 'impl: %s' % schema.get('impl')
    print 'dbtype: %s' % schema.get('dbtype')
    print 'version: %s' % schema.get('dbversion')
    print 'host: %s' % schema.get('host')
    print 'port: %s' % schema.get('port')
    print 'character_set: %s' % schema.get('character_set')
    print 'collation_type: %s' % schema.get('collation_type')
    print 'schema: %s' % schema.get('schema')
    print 'schema id: %d' % schema.get('schema_id')
    print('\033[0m')


def delete():
    CONF.register_cli_opts(one_opts)
    CONF.register_cli_opts(del_opts)
    CONF(project='cmd')
    _client = client()
    body = None
    if CONF.unquotes:
        body = dict(unquotes=CONF.unquotes)
    code, result, data = prepare_results(_client.schemas_delete, CONF.database_id, CONF.schema,
                                         body=body)
    if code:
        print('\033[1;31;40m')
        print 'Fail, code %d, result %s' % (code, result)
        if data:
            print data
        print('\033[0m')
        sys.exit(1)
    schema = data[0]
    print('\033[1;32;40m')
    print 'Delete database schema success'
    print 'database id: %d' % schema.get('database_id')
    print 'impl: %s' % schema.get('impl')
    print 'dbtype: %s' % schema.get('dbtype')
    print 'version: %s' % schema.get('dbversion')
    print 'host: %s' % schema.get('host')
    print 'port: %s' % schema.get('port')
    print 'schema: %s' % schema.get('schema')
    print 'schema id: %d' % schema.get('schema_id')
    print('\033[0m')


def list():
    for opt in one_opts:
        if opt.name == 'schema':
            one_opts.remove(opt)
            break

    CONF.register_cli_opts(one_opts)
    CONF.register_cli_opts(index_opts)
    CONF(project='cmd')
    _client = client()

    body = {}
    for opt in index_opts:
        body.setdefault(opt.name, CONF[opt.name])

    code, result, data = prepare_results(_client.schemas_index,
                                         database_id=CONF.database_id,
                                         body=body)
    if code:
        print('\033[1;31;40m')
        print 'Fail, code %d, result %s' % (code, result)
        if data:
            print data
        print('\033[0m')
        sys.exit(1)
    schema_heads = ['database_id', 'schema_id', 'schema', 'character_set', 'collation_type']
    tb = table.PleasantTable(ident=0, columns=schema_heads, counter=True)
    for schema in data:
        tb.add_row([schema.get('database_id'), schema.get('schema_id'), schema.get('schema'),
                    schema.get('character_set'), schema.get('collation_type')])
    print tb.pformat()


def show():
    CONF.register_cli_opts(one_opts)
    CONF(project='cmd')

    _client = client()
    code, result, data = prepare_results(_client.schemas_show, CONF.database_id,
                                         schema=CONF.schema,
                                         body=dict(secret=True, quotes=True))
    if code:
        print('\033[1;31;40m')
        print 'Fail, code %d, result %s' % (code, result)
        if data:
            print data
        print('\033[0m')
        sys.exit(1)

    schema = data[0]
    quotes = schema.get('quotes')
    print('\033[1;32;40m')
    print 'database id: %d' % schema.get('database_id')
    print 'impl: %s' % schema.get('impl')
    print 'dbtype: %s' % schema.get('dbtype')
    print 'version: %s' % schema.get('dbversion')
    print 'host: %s' % schema.get('host')
    print 'port: %s' % schema.get('port')
    print 'schema: %s' % schema.get('schema')
    print 'schema_id: %d' % schema.get('schema_id')
    print 'desc: %s' % schema.get('desc')
    print 'user: %s' % schema.get('user')
    print 'passwd: %s' % schema.get('passwd')
    print 'ro_user: %s' % schema.get('ro_user')
    print 'ro_passwd: %s' % schema.get('ro_passwd')
    if quotes:
        print 'quotes: %s' % str(quotes)
    print('\033[0m')


def main():
    FUNCS = ['list', 'create', 'show', 'delete']
    try:
        func = sys.argv.pop(1)
        if func not in FUNCS:
            raise ValueError
    except (IndexError, ValueError):
        print 'action is: %s' % '  '.join(FUNCS)
        print 'use -h for help'
        sys.exit(1)
    func = eval(func)
    logging.basicConfig(level=logging.WARN)
    CONF.register_cli_opts(client_opts)
    func()


if __name__ == '__main__':
    main()
