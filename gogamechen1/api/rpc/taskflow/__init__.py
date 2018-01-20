from goperation.manager.rpc.agent.application.taskflow.middleware import EntityMiddleware
from goperation.manager.rpc.agent.application.taskflow.database import Database


class GogameMiddle(EntityMiddleware):
    def __init__(self, entity, endpoint, objtype):
        super(GogameMiddle, self).__init__(entity, endpoint)
        self.objtype = objtype
        self.databases = {}
        self.dberrors = []
        self.waiter = None


class GogameCreateDatabase(Database):
    def __init__(self, **kwargs):
        super(GogameCreateDatabase, self).__init__(**kwargs)
        self.database_id = kwargs['database_id']
        self.source = kwargs['source']
        self.subtype = kwargs['subtype']
        self.ro_user = kwargs['ro_user']
        self.ro_passwd = kwargs['ro_passwd']
