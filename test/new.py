#!/usr/bin/python
# -*- encoding: utf-8 -*-
import json
import subprocess
import time
import datetime
import base64

def execute(cmd):
    print(cmd)


def main():

    date = '20190725'
    start = 81
    end = 120
    appfile = 'b7d36bb847ade92c623e710f6f2104fd'
    packages = [1, 7, 8, 9, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21, 22, 23, 24, 25, 27, 33, 45, 46, 51]

    group_id = 1
    cross = 2
    platform = 'android'

    packages = ','.join(map(str, packages))
    opentime = int(time.mktime(datetime.datetime.strptime(date, '%Y%m%d').timetuple()))

    with open('./servers.json', 'r') as f:
        _servers = json.load(f)

    servers = dict()
    for named in _servers:
        servers[named['id']] = named['name']

    commands = []
    for show_id in range(start, end+1):

        areaname = base64.encodestring(servers[show_id])
        areaname = base64.urlsafe_b64encode(areaname)

        info = dict(
            appfile=appfile,
            opentime=opentime,
            group_id=group_id,
            cross=cross,
            show_id=show_id,
            areaname=areaname,
            platform=platform,
            packages=packages,


        )

        cmd = '/usr/bin/gogamechen1-appentity create -o gamesvr --appfile %(appfile)s --opentime %(opentime)d ' \
              '--group_id %(group_id)d --show_id %(show_id)d --areaname %(areaname)s --cross %(cross)d ' \
              '--platform %(platform)s --packages %(packages)s' % info
        commands.append(cmd)

    for _cmd in commands:
        execute(_cmd)


if __name__ == '__main__':
    main()
