#!/usr/bin/python
# -*- coding:utf-8 -*-
import contextlib

import mysql
import mysql.connector


@contextlib.contextmanager
def sqlconn(sockfile, user, passwd, schema=None, raise_on_warnings=True):
    kwargs = dict(user=user, passwd=passwd, unix_socket=sockfile,
                  raise_on_warnings=raise_on_warnings)
    if schema:
        kwargs['database'] = schema
    conn = mysql.connector.connect(**kwargs)
    try:
        yield conn
    except Exception:
        raise
    finally:
        conn.close()


def main():
    skfile = '/var/lib/mysql/mysql.sock'
    user = 'root'
    passwd = ''
    schema = 'gogamechen1'

    with sqlconn(skfile, user, passwd, schema) as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("select a.entity,a.agent_id,a.group_id,b.show_id,b.area_id,"
                       "b.areaname from appentitys a left join gameareas b on a.entity = b.entity  "
                       "where status = 0 and objtype='gamesvr' order by a.entity")
        areas = cursor.fetchall()
        cursor.close()

        cursor = conn.cursor(dictionary=True)
        cursor.execute("select a.entity,b.host from appentitys a left join areadatabases b on a.entity = b.entity "
                       "where a.status = 0 and a.objtype='gamesvr' and b.subtype='datadb' order by a.entity")
        databases = cursor.fetchall()
        cursor.close()

    dbentitys = {}
    for database in databases:
        dbentitys[database['entity']] = database['host']

    entity = None
    entitys = {}
    for area in areas:
        if area['entity'] != entity:
            entity = area['entity']
            entitys[entity] = {'agent_id': area['agent_id'], 'group_id': area['group_id'], 'dbhost': dbentitys[entity],
                               'areas': []}
        entitys[entity]['areas'].append({'area_id': area['area_id'], 'show_id': area['show_id'], 'areaname': area['areaname']})
    for entity in sorted(entitys.keys()):
        schema = 'gogamechen1_gamesvr_datadb_%d' % entity
        dbhost = entitys[entity]['dbhost']
        print('schema: %s\thost: %s' %(schema, dbhost))
        for area in entitys[entity]['areas']:
            print('\t%d:%s' % (area['show_id'], area['areaname']))


if __name__ == '__main__':
    main()
