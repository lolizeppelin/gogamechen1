import re
import os
import simplejson as json
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


def _format_database_url(database):
    info = dict(user=database.get('user'),
                passwd=database.get('passwd'),
                host=database.get('host'),
                port=database.get('port'),
                schema=database.get('schema'),
                character_set=database.get('character_set'))
    return '%(user)s:%(passwd)s@tcp(%(host)s:%(port)d)/%(schema)s?charset=%(character_set)s' % info


USER_LIMIT = common.REGEXUSER
PASSWORD_LIMIT = common.REGEXPASS
HOSTORIP_LIMIT = '[a-z0-9-.]'
SCHEMA_LIMIT = '[a-z]+?[a-z0-9-_]*?[a-z0-9]'
CHARACTER_LIMIT = '[a-z0-9]'

DBURIREGX = re.compile('(%s+?):(%s+?)@tcp\((%s+?):([0-9]+?)\)/(%s+?)\?charset=(%s+?)$' % \
                       (USER_LIMIT, PASSWORD_LIMIT, HOSTORIP_LIMIT, SCHEMA_LIMIT, CHARACTER_LIMIT))


def deacidizing(cfile, subtype):
    uri = load(cfile).get(MAPS[subtype])
    if not uri:
        raise ValueError('Database uri is None')
    match = re.match(DBURIREGX, uri)
    if not match:
        raise ValueError('deacidizing uri fail')
    info = dict(user=match.group(1),
                passwd=match.group(2),
                host=match.group(3),
                port=int(match.group(4)),
                schema=match.group(5),
                character_set=match.group(6))
    return info


def _format_chiefs(cfile, chiefs):
    if os.path.exists(cfile):
        conf = load(cfile)
        old = conf.pop('ConnAddrs')
    else:
        old = {}

    new = {}

    for chief in (common.GMSERVER, common.CROSSSERVER):
        if chief in chiefs:
            port = chiefs[chief]['ports'][0]
            if chief == common.GMSERVER:
                # if len(chiefs[chief]['ports']) != 2:
                #     raise ValueError('Port count of %s is not 2' % chief)
                port = chiefs[chief]['ports'][1]
            new.setdefault(chief, '%s:%d' % (chiefs[chief]['local_ip'], port))

    _chiefs = OrderedDict()
    for chief in (common.GMSERVER, common.CROSSSERVER):
        if chief in new:
            _chiefs.setdefault(chief, new[chief])
        elif chief in old:
            _chiefs.setdefault(chief, old[chief])
        else:
            raise ValueError('chief %s config can not be found' % chief)
    return _chiefs


def format_databases(objtype, cfile, databases):
    if not cfile and not databases:
        raise ValueError('No databases found')
    subtypes = common.DBAFFINITYS[objtype].keys()
    _databases = dict()
    if databases:
        for subtype in subtypes:
            database = databases[subtype]
            _databases.setdefault(subtype, _format_database_url(database))
    else:
        conf = load(cfile)
        for subtype in subtypes:
            database = conf.pop(MAPS[subtype])
            _databases.setdefault(subtype, database)
    return _databases


def format_chiefs(objtype, cfile, chiefs):
    if objtype != common.GAMESERVER:
        return None
    if not cfile and not chiefs:
        raise ValueError('No chiefs found')
    if chiefs:
        return _format_chiefs(cfile, chiefs)
    else:
        if not os.path.exists(cfile):
            raise ValueError('No config file found, need config of chiefs')
        conf = load(cfile)
        return conf.pop('ConnAddrs')


def server_flag(objtype, cfile, flag=None):
    if objtype != common.GAMESERVER:
        return None
    if not cfile and not flag:
        return None
    if flag is not None:
        return flag
    else:
        if not os.path.exists(cfile):
            return None
        conf = load(cfile)
        return conf.pop('ServerFlag', None)


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


def conf_type_1(logpath, local_ip, ports, entity, areas, databases, opentime, chiefs, flag=None):
    conf = OrderedDict()
    conf.setdefault('LogLevel', 'release')
    conf.setdefault('LogPath', logpath)
    conf.setdefault('TCPAddr', '%s:%d' % ('0.0.0.0', ports[0]))
    conf.setdefault('ListenAddr', '%s:%d' % (local_ip, ports[1]))
    conf.setdefault('RealServerId', entity)
    # conf.setdefault('ShowServerIds', [area.get('area_id') for area in areas])
    conf.setdefault('ShowServers', [dict(Id=area.get('area_id'),
                                         Name=area.get('areaname'),
                                         show_id=area.get('show_id')) for area in areas])
    if flag is not None:
        conf.setdefault('ServerFlag', flag)
    conf.setdefault('StartServerTime', opentime)
    conf.setdefault('ConnAddrs', chiefs)
    conf.setdefault('DB', databases[common.DATADB])
    conf.setdefault('DBMaxConn', GAMESERVER_DBPOOL_SIZE)
    conf.setdefault('LogDB', databases[common.LOGDB])
    return conf


def conf_type_2(logpath, local_ip, ports, entity, databases):
    conf = OrderedDict()
    conf.setdefault('LogLevel', 'release')
    conf.setdefault('LogPath', logpath)
    # conf.setdefault('WSAddr', '%s:%d' % (local_ip, ports[0]))
    conf.setdefault('WSAddr', '%s:%d' % ('0.0.0.0', ports[0]))
    conf.setdefault('ListenAddr', '%s:%d' % (local_ip, ports[1]))
    conf.setdefault('DB', databases[common.DATADB])
    return conf


def conf_type_3(logpath, local_ip, ports, entity, databases):
    conf = OrderedDict()
    conf.setdefault('LogLevel', 'release')
    conf.setdefault('LogPath', logpath)
    conf.setdefault('ListenAddr', '%s:%d' % (local_ip, ports[0]))
    conf.setdefault('DB', databases[common.DATADB])
    conf.setdefault('DBMaxConn', CROSSSERVER_DBPOOL_SIZE)
    return conf


CONF_MAKE = {common.GMSERVER: conf_type_2,
             common.CROSSSERVER: conf_type_3,
             common.GAMESERVER: conf_type_1}


def make(objtype, logpath,
         local_ip, ports,
         entity, areas, databases,
         opentime, chiefs, flag=None):
    if objtype == common.GAMESERVER:
        args = (logpath, local_ip, ports, entity, areas, databases, opentime, chiefs, flag)
    elif objtype == common.GMSERVER:
        args = (logpath, local_ip, ports, entity, databases)
    elif objtype == common.CROSSSERVER:
        args = (logpath, local_ip, ports, entity, databases)
    else:
        raise RuntimeError('Objtype error')
    func = CONF_MAKE[objtype]
    return func(*args)
