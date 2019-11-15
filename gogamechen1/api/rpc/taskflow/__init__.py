import os
import base64

from simpleutil.utils import digestutils

from goperation.filemanager import LocalFile
from goperation.manager.rpc.agent.application.taskflow.middleware import EntityMiddleware
from goperation.manager.rpc.agent.application.taskflow.database import Database
from goperation.manager.rpc.agent.application.taskflow.application import AppUpgradeFile
from goperation.manager.rpc.agent.application.taskflow.application import AppLocalBackupFile

from gogamechen1.api import gfile


class GogameMiddle(EntityMiddleware):

    def __init__(self, entity, endpoint, objtype):
        super(GogameMiddle, self).__init__(entity, endpoint)
        self.objtype = objtype
        self.databases = {}
        self.waiter = None


class GogameDatabase(Database):
    def __init__(self, **kwargs):
        super(GogameDatabase, self).__init__(**kwargs)
        self.database_id = kwargs.get('database_id')
        self.source = kwargs.get('source')
        self.rosource = kwargs.get('rosource')
        self.subtype = kwargs.get('subtype')
        self.ro_user = kwargs.get('ro_user')
        self.ro_passwd = kwargs.get('ro_passwd')


class GogameAppFile(AppUpgradeFile):

    def __init__(self, source, objtype, revertable=False, rollback=False,
                 stream=None):
        super(GogameAppFile, self).__init__(source, revertable, rollback)
        self.objtype = objtype
        self.stream = stream

    def post_check(self):
        gfile.check(self.objtype, self.file)

    def clean(self):
        if self.stream:
            os.remove(self.file)

    def prepare(self, middleware=None, timeout=None):
        if self.stream:
            if len(self.stream) > 40000:
                raise ValueError("Strem over size")
            file_path = os.path.join('/tmp', '%s.zip' % self.source)
            data = base64.b64decode(self.stream)
            if digestutils.strmd5(data) != self.source:
                raise ValueError('Md5 not match')
            with open(file_path, 'wb') as f:
                data = base64.b64decode(self.stream)
                f.write(data)
            self.localfile = LocalFile(file_path, self.source, len(data))
        else:
            self.localfile = middleware.filemanager.get(self.source, download=True, timeout=timeout)
        try:
            self.post_check()
        except Exception:
            localfile = self.localfile
            self.localfile = None
            if self.stream:
                os.remove(localfile.path)
            else:
                middleware.filemanager.delete(self.source)
            raise


class GogameAppBackupFile(AppLocalBackupFile):

    def __init__(self, destination, objtype):
        super(GogameAppBackupFile, self).__init__(destination,
                                                  exclude=gfile.CompressConfAndLogExcluder(),
                                                  topdir=False,
                                                  native=True)
        self.objtype = objtype

    def post_check(self):
        gfile.check(self.objtype, self.file)
