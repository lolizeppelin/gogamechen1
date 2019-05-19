-- 注册接口需要删除缓存
local driver = require "driver"
local config = require "config"

local ok, err
ok, err = driver:init(config)
if not ok then
	ngx.say(err)
	ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
end

driver:register()
