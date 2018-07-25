import os
import re
import eventlet
import zipfile
import tarfile

from simpleutil.utils.zlibutils.excluder import Excluder

from gogamechen1 import common


exclude_regx = re.compile('(.*?/)*?conf(/.*)?$|.*?\.log$')

REGEX = {
    common.GAMESERVER: re.compile('^bin(/|/libbehaviac.so|/libgointerface.so|/gamesvr)?$|'
                                  '^behaviac(/|/(?!.*?[\.])[\S]+?|/[\S]+?\.xml)?$|'
                                  '^(config|geology)(/|/[\S]+?\.json)?$'),
    common.CROSSSERVER: re.compile('^bin(/|/%s)?$' % common.CROSSSERVER),
    common.GMSERVER: re.compile('^bin(/|/%s)?$' % common.GMSERVER),
}


def exclude_by_name(pathname):
    if not pathname:
        return False
    if re.match(exclude_regx, pathname):
        return True
    return False


class CompressConfAndLogExcluder(Excluder):
    def __call__(self, compretype, shell=False):
        """find excluder function"""
        if shell:
            raise TypeError('No shell excluder for %s' % self.__class__.__name__)
        if compretype not in ('gz', 'bz', 'zip'):
            raise TypeError('No excluder for %s' % compretype)
        return exclude_by_name


class ExtractConfAndLogExcluder(Excluder):
    @staticmethod
    def gz_excluder(gzinfo):
        return exclude_by_name(gzinfo.name)

    @staticmethod
    def zip_excluder(zipinfo):
        return exclude_by_name(zipinfo.filename)

    def __call__(self, compretype, shell=False):
        """find excluder function"""
        if shell:
            raise TypeError('No shell excluder for %s' % self.__class__.__name__)
        if compretype == 'gz':
            return self.gz_excluder
        elif common == 'bz':
            return self.gz_excluder
        elif compretype == 'zip':
            return self.zip_excluder
        raise TypeError('No excluder for %s' % compretype)


def objtype_checker(objtype, filename):
    if not re.match(REGEX[objtype], filename):
        raise ValueError('%s not for %s' % (filename, objtype))


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
    for name in nameiter(filepath):
        objtype_checker(objtype, name)
        count += 1
        if count >= 100:
            count = 0
            eventlet.sleep(0)
