# -*- coding:utf-8 -*-
import time
from websocket import create_connection

from simpleutil.config import cfg
from goperation import config

from goperation.api.client import ManagerClient

from gogamechen1.api.client import GogameChen1DBClient
from gogamechen1 import common
import os
from simpleutil.utils import digestutils
from goperation.manager import common as manager_common

from gopcdn.utils import build_fileinfo

a = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\goperation.conf'
b = 'C:\\Users\\loliz_000\\Desktop\\etc\\goperation\\gcenter.conf'
config.configure('test', [a, b])

# wsgi_url = '127.0.0.1'
wsgi_url = '172.31.0.110'
wsgi_port = 7999

httpclient = ManagerClient(wsgi_url, wsgi_port, timeout=30)

client = GogameChen1DBClient(httpclient)


def file_create(objtype, version, path):
    md5 = digestutils.filemd5(path)
    crc32 = digestutils.filecrc32(path)
    size = os.path.getsize(path)
    ext = os.path.splitext(path)[1][1:]
    filename = os.path.split(path)[1]
    fileinfo = {'size': size,
                'crc32': crc32,
                'md5': md5,
                'ext': ext,
                'filename': filename,
                }

    body = {'timeout': 35}

    ret = client.objfile_create(objtype, 'appfile', version, fileinfo=fileinfo, body=body)['data'][0]

    print 'create cdn result %s' % str(ret)

    uri = ret.get('uri')
    import time
    time.sleep(0.1)
    ws = create_connection("ws://%s:%d" % (uri.get('ipaddr'), uri.get('port')),
                           subprotocols=["binary"])
    print "connect websocket success"
    _start = time.time()
    with open(path, 'rb') as f:
        while True:
            buffer = f.read(4096)
            if buffer:
                ws.send(buffer)
            else:
                print 'file send finish, size %d, time use %d' % (size, int(time.time()) - _start)
                break


def file_index():
    for r in client.objfiles_index()['data']:
        print r


def send_file(uuid, objtype):
    print client.objfile_send(uuid, objtype,
                              body={'request_time': int(time.time())})


def resource_show(resource_id):
    print client.cdnresource_show(resource_id=resource_id)


def resource_index():
    for r in client.cdnresource_index()['data']:
        # print r
        resource_show(r.get('resource_id'))


def resource_shows(resource_id):
    print client.cdnresource_shows(resource_id=resource_id)


def package_all():
    print client.package_all()


def group_index():
    print client.groups_index()


def pfile_create(package_id, apkpath):
    pfile_body = {'ftype': 'small', 'gversion': '1.11525',
                  'fileinfo': build_fileinfo(apkpath),
                  'timeout': 600,
                  'desc': '测试游戏程序包'}
    r = client.packagefile_create(package_id, body=pfile_body)
    print r
    if r.get('resultcode') != 0:
        print r
        return
    info = r['data'][0]
    uri = info.get('uri')
    print 'update to %s' % str(uri)
    import time
    time.sleep(0.1)
    ws = create_connection("ws://%s:%d" % (uri.get('ipaddr'), uri.get('port')),
                           subprotocols=["binary"])
    print "connect websocket success"
    _start = time.time()
    with open(apkpath, 'rb') as f:
        while True:
            buffer = f.read(40000)
            if buffer:
                ws.send(buffer)
            else:
                print 'file send finish, size %d, time use %d' % (os.path.getsize(apkpath),
                                                                  int(time.time()) - _start)
                break


def pacakge_create(group_id, resource_id, package_name, mark):
    print client.package_create(group_id, resource_id, package_name, mark)


def pacakge_index(group_id):
    print client.package_index(group_id)


gamepath = r'C:\Users\loliz_000\Desktop\game.zip'
gmpath = r'C:\Users\loliz_000\Desktop\gm.zip'
crosspath = r'C:\Users\loliz_000\Desktop\cross.zip'

# file_create(common.GAMESERVER, '1110', gamepath)
# file_create(common.GMSERVER, '1110', gmpath)
# file_create(common.CROSSSERVER, '1110', crosspath)

# file_index()
# send_file(uuid='6cad91aa-b3e5-4252-a860-6c19e68e037e', objtype='gamesvr')
# send_file(uuid='f6512b24-82f3-4886-bd08-5a7cb0e48fdb', objtype='loginsvr')
# send_file(uuid='b083e3b2-765b-4d47-9f65-75f23651eda8', objtype='publicsvr')

# group_index()
# resource_index()
resource_shows(resource_id=6)
resource_show(resource_id=6)

# pacakge_create(resource_id=6, group_id=1, package_name='com.packmon.dyb', mark='dyb')
# pacakge_index(group_id=1)


apkpath = r'C:\Users\loliz_000\Desktop\online.1.0.341.3046749070.apk'

# pfile_create(package_id=1, apkpath=apkpath)
package_all()
