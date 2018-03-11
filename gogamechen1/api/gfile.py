import os
import re
import eventlet
import zipfile
import tarfile
from gogamechen1 import common

gamesvr_regx = re.compile('^bin(/|/libbehaviac.so|/libgointerface.so|/gamesvr)?$|'
                          '^behaviac(/|/(?!.*?[\.])[\S]+?|/[\S]+?\.xml)?$|'
                          '^(config|geology)(/|/[\S]+?\.json)?$')

exclude_regx = re.compile('(.*?/)*?conf(/.*)?$|.*?\.log$')


def exclude(pathname):
    if not pathname:
        return False
    if re.match(exclude_regx, pathname):
        return True
    return False


def gamesvr_checker(filename):
    if not re.match(gamesvr_regx, filename):
        raise ValueError('%s not for gamesvr' % filename)


def loginsvr_checker(filename):
    if filename not in ('bin', 'bin/', 'bin/loginsvr'):
        raise ValueError('%s not for loginsvr' % filename)


def publicsvr_checker(filename):
    if filename not in ('bin', 'bin/', 'bin/publicsvr'):
        raise ValueError('%s not for publicsvr' % filename)


def nameiter(filepath):
    ext = os.path.splitext(filepath)[1][1:]
    with open(filepath, 'rb') as f:
        if ext == 'zip':
            objtarget = zipfile.ZipFile(file=f)
            for info in objtarget.infolist():
                yield info.filename
        else:
            objtarget = tarfile.TarFile.open(fileobj=f)
            for tarinfo in objtarget:
                yield tarinfo.name


def check(objtype, filepath):
    count = 0
    if objtype not in common.ALLTYPES:
        raise ValueError('objtype value error')
    _checker = eval('%s_checker' % objtype)
    for name in nameiter(filepath):
        _checker(name)
        count += 1
        if count >= 100:
            count = 0
            eventlet.sleep(0)
