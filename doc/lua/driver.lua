-- 公共数据接口
local cjson = require "cjson.safe"
local asynclock = require "resty.lock"
local redis = require "resty.redis"
local mysql = require "resty.mysql"

local ffi      = require "ffi"
local ffi_cast = ffi.cast
local C        = ffi.C

ffi.cdef[[
typedef unsigned char u_char;
uint32_t ngx_murmur_hash2(u_char *data, size_t len);
]]

function murmurhash2(value)
    return tonumber(C.ngx_murmur_hash2(ffi_cast('uint8_t *', value), #value))
end



local _M = {
    ["lock"] = ngx.null,
    ["jdata"] = ngx.null,
    ["config"] = ngx.null,
}

function mysql_conn(config)
    local conn = mysql:new()
    conn:set_timeout(config.timeout or 1000)
    local ok, err, errcode, sqlstate = conn:connect(config)  -- mysql 插件内部会自己解决是否需要新开链接还是从池中取
    if not ok then
        ngx.log(ngx.ERR, "failed to connect: ", err, ": ", errcode, " ", sqlstate)
        return nil
    end
    return conn
end

-- redis set keepalive
function keepalive(conn)
    local ok, err = conn:set_keepalive(_M.config.idle, _M.config.pool)
    if not ok then
        ngx.log(ngx.ERR, 'Set redis keepalive fail:' .. err)
        conn:close()
    end
end


function getcache(conn, key)
    local buffer, _ = conn:get(key)
    if not buffer then
        ngx.log(ngx.ERR, "Get data from cache fail: " .._)
        return nil, "Get data from cache fail: " .._
    end
    keepalive(conn)
    if buffer == ngx.null then
        return nil, nil
    end
    local data = cjson.decode(buffer)
    if not data then
        ngx.log(ngx.ERR, "Gat data not json")
        return nil, "Cache data not json"
    end
    return data, nil
end


function delcache(conn, key)
    local ret, _ = conn:del(key)
    if not ret then
        ngx.log(ngx.ERR, "Delete from cache fail: " .. _)
    else
        keepalive(conn)
    end
    return ret, _
end


function setcache(conn, key, data)
    local buffer = cjson.encode(data)
    if not buffer then
        ngx.log(ngx.ERR, "Set cache fail, data not json")
        keepalive(conn)
        return nil, 'Json encode fail'
    end
    local ok, err = conn:set(key, buffer, 'ex', '3600')
    if not ok then
        ngx.log(ngx.ERR, "Set cache fail: " .. err)
        return nil, "Set cache fail: " .. err
    end
    keepalive(conn)
    return ok, err
end


-- 配置初始化
function _M:init(conf)
    if _M.config ~= ngx.null then
        return true, nil
    end
    ngx.log(ngx.INFO, 'Init driver config')
    opts = {
        ["timeout"] = conf.locktimeout or 5
    }
    if _M.lock == ngx.null then
        local lock, err = asynclock:new(conf.dict, opts)
        if not lock then
            ngx.log(ngx.ERR, 'Init worker global lock fail: ' .. err)
            return nil, 'Init global lock fail'
        end
        _M.lock = lock
    end

    local config = {}
    -- 参数初始化 --
    config.prefix = conf.dict                   -- 必要参数,全局锁用到共享字典名,顺便作为前缀
    config.timeout = conf.timeout or 1000       -- 1000  全局timeout,单位ms
    config.murmur = conf.murmur
    -- config of redis --
    config.path = conf.path
    config.host = conf.host or '127.0.0.1'          -- 127.0.0.1
    config.port = conf.port or 6379                 -- 6379
    config.db = conf.db or 0                        -- 默认0
    config.passwd  = conf.passwd
    config.key  = conf.key                          -- 必要参数,servers列表所在key

    -- config of mysql --
    config.dbpath  = conf.dbpath
    config.dbhost  = conf.dbhost or '127.0.0.1'     -- 127.0.0.1
    config.dbport  = conf.dbport or 3306            -- 3306
    config.schema  = conf.schema                    -- 必要参数
    config.dbuser  = conf.dbuser                    -- 必要参数
    config.dbpass  = conf.dbpass                    -- 必要参数
    config.idle  = conf.idle or 60000               -- 60000ms
    config.pool  = conf.pool or 10                  -- 10
    -- mysql table info --
    config.table  = conf.table                      -- areas   查询表名
    config.coluid  = conf.coluid                    -- uid     uid列名
    config.colsid  = conf.colsid                    -- area    area列名
    config.coltime  = conf.coltime                  -- area    area添加时间列名

    -- config of caches --
    config.caches = {}
    config.murmur = conf.murmur                 -- 默认false, 是否使用murmurhash2散布uid
    if conf.caches then                         -- caches  redis配置列表

        for index, _conf in ipairs(conf.caches) do
            config.caches[index] = {
                ["path"] = _conf.path,
                ["host"] = _conf.host or '127.0.0.1',
                ["port"] = _conf.port or 6379,
                ["db"] = _conf.db or 0,
                ["passwd"] = _conf.passwd,
            }
        end
    end

    -- 简单校验 禁止长度不一致
    if #config.caches ~= #conf.caches then
        return nil, 'cache config error'
    end

    if #config.caches <= 0 then
        config.caches = ngx.null
    end

    ngx.log(ngx.INFO, 'Cache config size: ' .. #config.caches)
    _M.config = config
    return true, nil
end

-- redis 连接池初始化
function _M:redisconnect(config)
    if config == ngx.null or not config then
        return nil
    end

    local conn = redis:new()
    conn:set_timeout(config.timeout or 1000)
    local ok, err, count
    if config.path then
        ok, err = conn:connect(string.format("unix:%s", config.path))
    else
        ok, err = conn:connect(config.host, config.port)
    end
    if not ok then
        ngx.log(ngx.ERR, 'Connect to redis fail:' .. err)
        return nil
    end
    -- 检查当前redis链接是否新建链接
    count, err = conn:get_reused_times()
    -- 新建链接 处理认证&数据库选择
    if count == 0 then
        if config.passwd then
            ok, err = conn:auth(config.passwd)
            if not ok then
                conn:close()
                ngx.log(ngx.ERR, 'Redis authenticate fail:' .. err)
                return nil
            end
        end
        ok, err = conn:select(tostring(config.db))
        if not ok then
            conn:close()
            ngx.log(ngx.ERR, 'Select redis db fail:' .. err)
            return nil
        end
    elseif count == nil then
        conn:close()
        ngx.log(ngx.ERR, 'Get redis connection reused times fail:' .. err)
        return nil
    end
    return conn
end

-- mysql 连接池初始化
function _M:mysqlconnect(config)
    if config == ngx.null or not config then
        return nil
    end

    local _config = {}
    if config.dbpath then
        _config.path = config.dbpath
    else
        _config.host = config.dbhost
        _config.port = config.dbport
    end
    _config.database = config.schema
    _config.user = config.dbuser
    _config.password  = config.dbpass
    _config.timeout  = config.timeout

    return mysql_conn(_config)
end

-- servers数据初始化
function _M:fetchservers()
    if _M.config == ngx.null then
        return nil
    end
    local lock = _M.lock
    local key =  _M.config.prefix .. "-redis-servers-" .. ngx.worker.id()
    local elapsed, _ = lock:lock(key)
    if elapsed == nil then
        ngx.log(ngx.ERR, 'Get global lock for redis fail')
        return nil
    end
    if _M.jdata ~= ngx.null then
        lock:unlock()
        return nil
    end

    local conn = _M:redisconnect(_M.config)
    if not conn then
        lock:unlock()
        return nil
    end
    local serverkey =  _M.config.prefix .. '-' .._M.config.key
    local data, err = conn:get(serverkey)
    conn:close()
    if not data then
        ngx.log(ngx.ERR, 'Get Redis key error:' .. err)
        lock:unlock()
        return nil
    end
    if data == ngx.null then
        ngx.log(ngx.ERR, 'Redis key not found, key: ' .. serverkey)
        lock:unlock()
        return nil
    end
    local r = cjson.decode(data);
    if r ~= nil then
        _M.jdata = r
    else
        ngx.log(ngx.ERR, 'Data from redis is not json')
    end
    lock:unlock()
    return nil
end

-- 从redis获取服务器列表
function _M:getservers()
    if _M.jdata == ngx.null then
        _M:fetchservers()
    end
    return _M.jdata
end

function _M:getcache(uid)
    local config = _M.config
    local caches = config.caches
    if caches == ngx.null then
        return nil, nil, 'cache not enable'
    end

    local hashid
    if config.murmur then
        hashid = murmurhash2(tostring(uid))
    else
        hashid = uid
    end
    local index = hashid % #caches
    index = index + 1
    local uidkey = config.prefix .. '-cache-uid-' ..hashid

    local _config = caches[index]
    local conn = _M:redisconnect(_config)
    if not conn then
        return nil, uidkey, 'connect to redis cache fail'
    end
    return conn, uidkey, nil
end

-- 获取用户areas信息
function _M:getareas(uid, cache)
    local config = _M.config
    if config ==  ngx.null then
        return nil, 'config not init'
    end
    local caches = config.caches

    local flush  = false
    local conn, key, _

    -- fetch areas from redis
    if caches ~= ngx.null then
        conn, key, _ = _M:getcache(uid)
        if not conn then
            ngx.log(ngx.ERR, "Get connection for cache fail: " .. _)
        end
        if cache and conn then
            local res, _ = getcache(conn, key)
            if res then
                return res, nil
            end
            flush = true                   -- no cache found
        elseif conn and not cache then     -- delete cache
            ngx.thread.spawn(delcache, conn, key)
        end
    end

    -- fetch areas from mysql
    local db = _M:mysqlconnect(config)
    local sql = string.format("select %s as uid, %s as area from %s where %s = %d order by %s desc",
            config.coluid, config.colsid, config.table, config.coluid, uid, config.coltime)
    ngx.log(ngx.DEBUG, "sql: " .. sql)
    local res, err, errcode, sqlstate = db:query(sql)
    if not res then
        ngx.log(ngx.ERR, "mysql query error: ", err, ": ", errcode, ": ", sqlstate, ".")
        return nil, 'Mysql execute query error'
    end

    local ok, _ = db:set_keepalive(config.idle, config.pool)
    if not ok then
        ngx.log(ngx.ERR, "failed to set mysql keepalive: ", _)
        db:close()
    end
    -- flush areas into cache
    if #res > 0 and flush then
        ngx.thread.spawn(setcache, conn, key, res)
    end
    return res, nil
end

-- 注册接口调用的时候删除缓存中用户记录
function _M:register(uid)
    local config = _M.config
    if config == ngx.null then
        return
    end
    local caches = config.caches
    if caches ~= ngx.null then
        local conn, key, _ = _M:getcache(uid)
        if conn then
            ngx.thread.spawn(delcache, conn, key)
        else
            ngx.log(ngx.ERR, "Get connection for cache fail: " .. _)
        end
    end
end


return _M