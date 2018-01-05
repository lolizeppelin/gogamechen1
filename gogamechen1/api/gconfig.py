import json
from gogamechen1 import common
from collections import OrderedDict

__version__ = 1

GAMESERVER_DBPOOL_SIZE = 3
CROSSSERVER_DBPOOL_SIZE = 5

MAPS = {common.DATADB: 'DB',
        common.LOGDB: 'LogDB'}


def load(cfile):
    with open(cfile, 'rb') as f:
        return json.load(f, object_pairs_hook=OrderedDict)


def _format_database_url(subtype, database):
    info = dict(user=database.get('user'),
                passwd=database.get('passwd'),
                host=database.get('host'),
                port=database.get('port'),
                schema=database.get('schema'),
                character_set=database.get('character_set'))
    return '%(user)s:%(passwd)s@tcp(%(host)s:%(port)d)/%(schema)s?charset=%(character_set)s' % info


def _format_chiefs(chiefs):
    _chiefs = OrderedDict()
    for chief in (common.GMSERVER, common.CROSSSERVER):
        _chiefs.setdefault(chief, '%s:%d' % (chiefs[chief]['local_ip'],
                                             chiefs[chief]['ports'][0]))
    return _chiefs


def format_databases(objtype, cfile, databases):
    if not cfile and not databases:
        raise ValueError('No databases found')
    subtypes = common.DBAFFINITYS[objtype].keys()
    _databases = dict()
    if databases:
        for subtype in subtypes:
            database = databases[subtype]
            _databases.setdefault(subtype, _format_database_url(subtype, database))
    else:
        conf = load(cfile)
        for subtype in subtypes:
            database = conf.pop(MAPS[subtype])
            _databases.setdefault(subtype, _format_database_url(subtype, database))
    return _databases


def format_chiefs(objtype, cfile, chiefs):
    if objtype != common.GAMESERVER:
        return None
    if not cfile and not chiefs:
        raise ValueError('No chiefs found')
    if chiefs:
        return _format_chiefs(chiefs)
    else:
        conf = load(cfile)
        return conf.pop('ConnAddrs')


def format_opentime(objtype, cfile, opentime):
    if objtype != common.GAMESERVER:
        return None
    if not cfile and not opentime:
        raise ValueError('No opentime found')
    if opentime:
        return opentime
    else:
        conf = load(cfile)
        return conf.pop('StartServerTime')


def gamesvr_make(logpath, local_ip, ports, entity, areas, databases, opentime, chiefs):
    conf = OrderedDict()
    conf.setdefault('LogLevel', 'info')
    conf.setdefault('LogPath', logpath)
    conf.setdefault('TCPAddr', '%s:%d' % ('0.0.0.0', ports[0]))
    conf.setdefault('RealServerId', entity)
    conf.setdefault('ShowServerIds', areas)
    conf.setdefault('StartServerTime', opentime)
    conf.setdefault('ConnAddrs', chiefs)
    conf.setdefault('DB', databases[common.DATADB])
    conf.setdefault('DBMaxConn', GAMESERVER_DBPOOL_SIZE)
    conf.setdefault('LogDB', databases[common.LOGDB])
    return conf


def loginsvr_make(logpath, local_ip, ports, entity, databases):
    conf = OrderedDict()
    conf.setdefault('LogLevel', 'info')
    conf.setdefault('LogPath', logpath)
    conf.setdefault('WSAddr', '%s:%d' % (local_ip, ports[0]))
    conf.setdefault('ListenAddr', '%s:%d' % (local_ip, ports[1]))
    conf.setdefault('DB', databases[common.DATADB])
    return conf


def publicsvr_make(logpath, local_ip, ports, entity, databases):
    conf = OrderedDict()
    conf.setdefault('LogLevel', 'info')
    conf.setdefault('LogPath', logpath)
    conf.setdefault('ListenAddr', '%s:%d' % (local_ip, ports[0]))
    conf.setdefault('DB', databases[common.DATADB])
    conf.setdefault('DBMaxConn', CROSSSERVER_DBPOOL_SIZE)
    return conf


def make(objtype, logpath,
         local_ip, ports,
         entity, areas, databases,
         opentime, chiefs):
    if objtype == common.GAMESERVER:
        args = (logpath, local_ip, ports, entity, areas, databases, opentime, chiefs)
    elif objtype == common.GMSERVER:
        args = (logpath, local_ip, ports, entity, databases)
    elif objtype == common.CROSSSERVER:
        args = (logpath, local_ip, ports, entity, databases)
    else:
        raise RuntimeError('Objtype error')
    func = '%s_make' % objtype
    func = eval(func)
    return func(*args)
