local driver = require "driver"
local analyze = require "analyze"
local config = require "config"


local ok, err
ok, err = driver:init(config)
if not ok then
	ngx.say(err)
	ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
end

if not analyze:fetch() then
	ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
end
