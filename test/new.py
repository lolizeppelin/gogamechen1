#!/usr/bin/python
# -*- encoding: utf-8 -*-
import json
import time
import datetime
import base64

def execute(cmd):
    print(cmd)


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

    with open('./servers.json', 'r') as f:
        _servers = json.load(f)

    servers = dict()
    for named in _servers:
        servers[named['id']] = named['name']

    commands = []
    for show_id in range(start, end+1):
        areaname = base64.encodestring(servers[show_id].encode('utf8'))
        areaname = base64.urlsafe_b64encode(areaname)

        info = dict(
            appfile=appfile,
            opentime=opentime,
            group_id=group_id,
            cross=cross,
            show_id=show_id,
            areaname=areaname,
            platform=platform,
        )
        cmd = '/usr/bin/gogamechen1-appentity create -o gamesvr --appfile %(appfile)s --opentime %(opentime)d ' \
              '--group_id %(group_id)d --show_id %(show_id)d --areaname %(areaname)s --cross %(cross)d ' \
              '--platform %(platform)s' % info
        if exclude:
            cmd += ' --exclude %s' % ','.join(map(str, exclude))
        commands.append(cmd)

    for _cmd in commands:
        execute(_cmd)


if __name__ == '__main__':
    main()
