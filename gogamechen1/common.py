NAME = 'gogamechen1'


GAMESERVER = 'gamesvr'
GMSERVER = 'loginsvr'
CROSSSERVER = 'publicsvr'
ALLTYPES = set([GAMESERVER, GMSERVER, CROSSSERVER])

DATADB = 'datadb'
LOGDB = 'logdb'
APPFILE = 'appfile'

UNACTIVE = -1
OK = 0

APPAFFINITYS = {GAMESERVER: 1, CROSSSERVER: 2, GMSERVER: 4}
#    map app affinitys by bitwise operation
#    GM    CROSS  GAME
#     4      2      1
# GM & CROSS 6
# GM & CROSS & GAME

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
