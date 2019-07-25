#!/usr/bin/python
# -*- encoding: utf-8 -*-
import json
import time
import datetime
import base64
import subprocess
import json


def execute(cmd):
    print(cmd)


class Select(object):

    def __init__(self, data):
        self.index = 0
        self.max = len(data)
        self.data = data

    def get(self):
        tareget = self.data[self.index]
        _next = self.index + 1
        if _next >= self.max:
            _next = 0
        self.index = _next
        return tareget


def agent_and_database():
    cmd = 'gogamechen1-select  agents --format json'
    sub = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    agents = sub.communicate()[0]
    agents = json.loads(agents)
    agents = Select(agents)

    cmd = 'gogamechen1-select  databases --format json'
    sub = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    _databases = []

    databases = sub.communicate()[0]
    databases = json.loads(databases)

    for dbs in databases:
        if dbs['affinity'] & 3:
            _databases.extend(dbs['databases'])
    databases = Select(_databases)
    return agents, databases


def main():


    date = '20190725'
    start = 92
    end = 120
    appfile = 'b7d36bb847ade92c623e710f6f2104fd'

    exclude = []
    group_id = 1
    cross = 2
    platform = 'android'


    opentime = int(time.mktime(datetime.datetime.strptime(date, '%Y%m%d').timetuple()))
    agents, databsaes = agent_and_database()
    with open('./servers.json', 'r') as f:
        _servers = json.load(f)

    servers = dict()
    for named in _servers:
        servers[named['id']] = named['name']

    commands = []
    for show_id in range(start, end+1):
        areaname = base64.encodestring(servers[show_id].encode('utf8'))
        areaname = base64.urlsafe_b64encode(areaname)

        agent_id = agents.get()
        database_id = databsaes.get()

        info = dict(
            appfile=appfile,
            opentime=opentime,
            group_id=group_id,
            cross=cross,
            show_id=show_id,
            areaname=areaname,
            platform=platform,
            database_id=database_id,
            agent_id=agent_id,

        )
        cmd = '/usr/bin/gogamechen1-appentity create -o gamesvr --appfile %(appfile)s --opentime %(opentime)d ' \
              '--group_id %(group_id)d --show_id %(show_id)d --areaname %(areaname)s --cross %(cross)d ' \
              '--agent %(agent_id)d --datadb %(database_id)d --logdb  %(database_id)d ' \
              '--platform %(platform)s' % info
        if exclude:
            cmd += ' --exclude %s' % ','.join(map(str, exclude))
        commands.append(cmd)

    for _cmd in commands:
        execute(_cmd)


if __name__ == '__main__':
    main()
