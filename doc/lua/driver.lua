-- 公共数据接口
local ffi      = require "ffi"
local ffi_cast = ffi.cast
local C        = ffi.C

local cjson = require "cjson.safe"
local asynclock = require "resty.lock"
local redis = require "resty.redis"


ffi.cdef[[
typedef unsigned char u_char;
uint32_t ngx_murmur_hash2(u_char *data, size_t len);
]]

function murmurhash2(value)
    return tonumber(C.ngx_murmur_hash2(ffi_cast('uint8_t *', value), #value))
end


-- redis 连接池初始化
local function redisconnect(config)
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

-- cache 封装
local Cache = {}

function Cache:new(config, exptime)
    local obj = {}
    setmetatable(obj, self)
    self.__index = self
    self.config = config
    self.exptime = exptime

    if config.shared then                       -- nginx shared dict 缓存
        obj.setkey = self.redis_setkey
        obj.getkey = self.redis_getkey
        obj.delkey = self.redis_delkey
        obj.free = self.redis_free
        obj.shared = ngx.shared[config.shared]
    else                                       -- redis 缓存
        obj.setkey = self.shared_setkey
        obj.getkey = self.shared_getkey
        obj.delkey = self.shared_delkey
        obj.free = self.shared_free
        obj.conn = redisconnect(config)
    end

    return obj
end

-- redis
function Cache:redis_getkey(key)
    return self.conn:get(key)
end

function Cache:redis_setkey(key, raw)
    local ok, _ = self.conn:set(key, raw, 'ex', self.exptime)
    return ok
end

function Cache:redis_delkey(key)
    local ok, _ = self.conn:del(key)
    return ok
end

function Cache:redis_free()
    local ok, err = self.conn:set_keepalive(self.config.idle, self.config.pool)
    if not ok then
        ngx.log(ngx.ERR, 'Set redis keepalive fail:' .. err)
        self.conn:close()
    end
end

-- shared dict
function Cache:shared_getkey(key)
    local raw, err = self.shared:get(key)
    if not raw and err then
        return nil, 'Share get key fail: ' .. err
    end
    if not raw then
        return ngx.null, nil
    end
    return raw, nil
end

function Cache:shared_setkey(key, raw)
    local success, err, _ self.shared:set(key, raw)
    if not success then
        ngx.log(ngx.ERR, 'Share set key fail: ' .. err)
        return nil
    end
    self.shared:expire(key, self.exptime)
end

function Cache:shared_delkey(key)
    self.shared:get(key)
end

function Cache:shared_free()
    -- do nothing
end

-- 接口
function Cache:getrole(key)
    local raw, err = self.getkey(key)
    if not raw then
        ngx.log(ngx.ERR, 'Get role error: ' .. err)
        return nil
    end
    if raw == ngx.null then
        self.free()
        return nil
    end
    self.free()
    return raw
end

function Cache:addrole(key, raw)
    local jdata = cjson.decode(raw)
     if not jdata then
         self.free()
         return nil
    end
    local roles, _ = self.getkey(key)
    if not roles then
        return nil
    end
    if roles == ngx.null then
        roles = {
            [1] = jdata
        }
    else
        roles[#roles+1] = jdata
    end
    self.setkey(key, cjson.encode(roles))
    self.free()
end

function Cache:editrole(key, raw)
    local jdata = cjson.decode(raw)
     if not jdata then
         self.free()
         return nil
    end
    local roles, _ = self.getkey(key)
    if not roles then
        return nil
    end
    if roles == ngx.null then
        roles = {
            [1] = jdata
        }
    else
        roles = cjson.encode(roles)
        if not roles then
            self.delkey(key)
        end
        -- 循环匹配
    end
    self.setkey(key, cjson.encode(roles))
    self.free()
end

function Cache:setrole(key, raw)
    local roles = cjson.decode(raw)
     if not roles then
         self.free()
         return nil
    end
    self.setkey(key, raw)
    self.free()
end


-- 对外接口
local _M = {
    ["lock"] = ngx.null,
    ["config"] = ngx.null,
    ["shared"] = ngx.null,
}

-- 配置初始化
function _M:init(conf)
    if _M.config ~= ngx.null then
        return true, nil
    end

    ngx.log(ngx.INFO, 'Try init driver config')

    opts = {
        ["timeout"] = conf.locktimeout or 5
    }
    if _M.lock == ngx.null then
        local lock, err = asynclock:new(conf.dict, opts)
        if not lock then
            return nil, 'Init global lock fail: ' .. err
        end
        _M.lock = lock
    end

    if _M.shared == ngx.null then
        local shared = ngx.shared[conf.dict]
        if not shared then
            return nil, 'Init get shared dict fail'
        end
        _M.shared = shared
    end

    local config = {}
    -- 参数初始化 --
    config.prefix = conf.dict                   -- 必要参数,全局锁用到共享字典名,顺便作为前缀
    config.murmur = conf.murmur
    -- 全局过期参数
    config.exptime = conf.exptime or 90000          -- 90000,全局过期时间,单位s,默认25小时
    -- config of mysql --
    config.dbpath  = conf.dbpath
    config.dbhost  = conf.dbhost or '127.0.0.1'     -- 127.0.0.1
    config.dbport  = conf.dbport or 3306            -- 3306
    config.schema  = conf.schema                    -- 必要参数
    config.dbuser  = conf.dbuser                    -- 必要参数
    config.dbpass  = conf.dbpass                    -- 必要参数

    -- config of caches --
    config.caches = {}
    config.murmur = conf.murmur                 -- 默认false, 是否使用murmurhash2散布uid

    local cache_shared = {}

    if conf.caches then                         -- caches  redis配置列表
        for index, _conf in ipairs(conf.caches) do
            if _conf.shared then
                if cache_shared[_conf.shared] then
                    return nil, 'Init get cache shared dict duplicate'
                end
                if not ngx.shared[_conf.shared] then
                    return nil, 'Init get cache shared dict fail'
                end
                config.caches[index] = {
                    ["shared"] = _conf.shared
                }
                cache_shared[_conf.shared] = true
            else
                config.caches[index] = {
                    ["path"] = _conf.path,
                    ["host"] = _conf.host or '127.0.0.1',
                    ["port"] = _conf.port or 6379,
                    ["db"] = _conf.db or 0,
                    ["passwd"] = _conf.passwd,
                    ["idle"] = _conf.idle or 60000,
                    ["pool"] = _conf.pool or 20,
                    ["timeout"] = _conf.timeout or 1000,
                }
            end
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

-- 获取缓存链接对象
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
        hashid = tonumber(uid)
    end
    local index = hashid % #caches
    index = index + 1
    local uidkey = config.prefix .. '-cache-uid-' ..uid
    local _config = caches[index]
    local cache = Cache:new(_config, config.exptime)
    --if not cache then
    --    return nil, uidkey, 'Create cache instance fail'
    --end
    return cache, uidkey, nil
end

function _M:getservers()
    local servers, _ = _M.shared:get(_M.config.prefix .. '-all-servers')
    if not servers or servers == ngx.null or servers == '' then
        return nil
    end
    return servers
end

function _M:setservers(raw, flush)
    local jdata = cjson.decode(raw)
    if jdata and #jdata > 0 then
        if not flush then
            local lock = _M.lock
            local key =  _M.config.prefix .. "-all-servers-lock"
            local elapsed, _ = lock:lock(key)
            if elapsed == nil then
                ngx.log(ngx.ERR, 'Get global lock for redis fail')
                return nil
            end
            if self.getservers() then
                lock:unlock()
                return
            end
        end
        _M.shared:set(_M.config.prefix .. '-all-servers', raw)
        if not flush then
            lock:unlock()
        end
    end
end

function _M:getrole(uid)
    local cache, uidkey, _ = _M:getcache(uid)
    if not cache then
        return nil
    end
    return cache:getrole(uidkey)
end

function _M:setrole(uid, raw)
    local cache, uidkey, _ = _M:getcache(uid)
    if not cache then
        return nil
    end
    return cache:setrole(uidkey, raw)
end

function _M:addrole(uid, raw)
    local cache, uidkey, _ = _M:getcache(uid)
    if not cache then
        return nil
    end
    return cache:addrole(uidkey, raw)
end

function _M:editrole(uid, raw)
    local cache, uidkey, _ = _M:getcache(uid)
    if not cache then
        return nil
    end
    return cache:editrole(uidkey, raw)
end


return _M