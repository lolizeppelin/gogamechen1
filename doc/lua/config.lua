-- 配置参数
--[[
-- 参数初始化 --
config.prefix = conf.dict       -- 必要参数,全局锁用到共享字典名,顺便作为前缀
config.timeout = conf.timeout   -- 1000  全局timeout,单位ms

-- config of redis --
config.path = conf.path
config.host = conf.host         -- 127.0.0.1
config.port = conf.port         -- 6379
config.db = conf.db             -- 默认0
config.passwd  = conf.passwd
config.key  = conf.key          -- 必要参数,servers列表所在key

-- config of mysql --
config.dbpath  = conf.dbpath
config.dbhost  = conf.dbhost    -- 127.0.0.1
config.dbport  = conf.dbport    -- 3306
config.schema  = conf.schema    -- 必要参数
config.dbuser  = conf.dbuser    -- 必要参数
config.dbpass  = conf.dbpass    -- 必要参数
config.idle  = conf.idle        -- 60000ms
config.pool  = conf.pool        -- 10
-- mysql table info --
config.table  = conf.table                  -- areas   查询表名
config.coluid  = conf.coluid                -- uid     uid列名
config.colsid  = conf.colsid                -- area    area列名
config.coltime  = conf.coltime              -- area    area添加时间列名
config.cacheidle  = conf.cacheidle          -- 60000ms
config.cachepool  = conf.cachepool          -- 20      redis connection pool size
--]]


local config = {}
config.dict = 'gogamechen1'   -- redis key name
config.key = 'all-packages'   -- redis key name
config.murmur = true
-- mysql
config.schema = 'test'
config.dbuser = 'root'
config.dbpass = '111111'

-- redis
config.path  = '/run/redis/redis.sock'
-- caches
config.caches = {
    { path = '/run/redis/redis.sock', db = 1 }
}

return config


