from goperation.manager.rpc.agent.application.taskflow.middleware import EntityMiddleware
from goperation.manager.rpc.agent.application.taskflow.database import Database


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
        self.subtype = kwargs.get('subtype')
        self.ro_user = kwargs.get('ro_user')
        self.ro_passwd = kwargs.get('ro_passwd')
