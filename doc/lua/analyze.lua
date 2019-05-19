-- 数据解析
local driver = require "driver"

_ANALYZE = {}


-- 解析数据并返回
function _ANALYZE:fetch(path)
    local path = path or ''
    local jdata = driver:getservers()
    if not jdata or jdata == ngx.null then
        ngx.status = 400
        ngx.header["Content-Type"] = 'text/plain';
        ngx.say("No server found")
        return false
    end
    local areas, err = driver:getareas(1, true)
    if not areas then
        ngx.log(ngx.ERR, "Get areas error: " .. err)
    end
    ngx.header["Content-Type"] = 'application/json';
    ngx.status = 200
    ngx.say(jdata)
    ngx.eof()
    return true
end

return _ANALYZE
