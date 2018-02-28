NAME = 'gogamechen1'

REGEXUSER = '[A-Za-z]+?[A-Za-z-_0-9]*?[A-Za-z0-9]'
REGEXPASS = '[a-zA-Z0-9-_.]'

GAMESERVER = 'gamesvr'
GMSERVER = 'loginsvr'
CROSSSERVER = 'publicsvr'
ALLTYPES = frozenset([GAMESERVER, GMSERVER, CROSSSERVER])

DATADB = 'datadb'
LOGDB = 'logdb'
APPFILE = 'appfile'

UNACTIVE = -1
OK = 0
DELETED = -2

APPAFFINITYS = {GAMESERVER: 1, CROSSSERVER: 2, GMSERVER: 4}
#    map app affinitys by bitwise operation
#    GM    CROSS  GAME
#     4      2      1
# GAME 1
# CROSS 2
# GM 4
# GM & CROSS 6
# GM & CROSS & GAME 7

DBAFFINITYS = {GAMESERVER: {DATADB: 1, LOGDB: 2},
               CROSSSERVER: {DATADB: 4}, GMSERVER: {DATADB: 8},}

#    map database affinitys by bitwise operation
#
#    GM-DATADB    CROSS-DATA-DB   GAME-LOGDB    GAME-DATADB
#         1             1              1            1
#         0             0              0            0
#
# GAME-DATADB 2**0 = 1
# GAME-LOGDB 2**1 = 2
# CROSS-DATA-DB = 2**2 = 4
# GM-DATADB = 2**3 = 8
#
#
# GAME-LOGDB & GAME-LOGDB = 3
# CROSS-DATA-DB & GM-DATADB = 12
#
# affinity & DBAFFINITYS[GAMESERVER][DATADB]


# package static var

ENABLE = 1
DISABLE = 0

ANY = 'any'
ANDROID = 'android'
IOS = 'ios'

# EntityTypeMap = {IOS: 'ios',
#                  ANDROID: 'android',
#                  ANY: 'any'}
#
# InvertEntityTypeMap = dict([(v, k) for k, v in EntityTypeMap.iteritems()])

SMALL_PACKAGE = 'small'
UPDATE_PACKAGE = 'update'
FULL_PACKAGE = 'full'

# PackageTypeMap = {SMALL_PACKAGE: 'small',
#                   UPDATE_PACKAGE: 'update',
#                   FULL_PACKAGE: 'full'}

# from itertools import izip
# InvertPackageTypeMap = dict(izip(PackageTypeMap.itervalues(), PackageTypeMap.iterkeys()))
# InvertPackageTypeMap = dict([(v, k) for k, v in PackageTypeMap.iteritems()])
