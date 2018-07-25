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
    def __init__(self, source, objtype, revertable=False, rollback=False):
        super(GogameAppFile, self).__init__(source, revertable, rollback)
        self.objtype = objtype

    def post_check(self):
        gfile.check(self.objtype, self.file)


class GogameAppBackupFile(AppLocalBackupFile):

    def __init__(self, destination, objtype):
        super(GogameAppBackupFile, self).__init__(destination,
                                                  exclude=gfile.CompressConfAndLogExcluder(),
                                                  topdir=False,
                                                  native=True)
        self.objtype = objtype

    def post_check(self):
        gfile.check(self.objtype, self.file)
