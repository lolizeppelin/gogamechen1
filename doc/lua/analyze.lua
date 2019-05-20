-- 数据解析
local driver = require "driver"

local _ANALYZE = {}


-- 解析数据并返回
function _ANALYZE:fetch(uid)
	local data
    local servers = driver:getservers()
    if not servers or servers == ngx.null then
        return flase
    end
    local user, err = driver:getuser(uid, true)
    if not user then
        ngx.log(ngx.ERR, "Get areas error: " .. err)
		return false
    end
	data.Servers = servers
	data.usr = user
    ngx.say(data)
    return true
end


function _ANALYZE:delete(uid)
    driver:deleteuser(uid)
end


function _ANALYZE:filter(config)
	local ok, err = driver:init(config)
	if not ok then
		ngx.log('Init lua cache driver fail:' .. err)
		return ngx.exit(ngx.HTTP_OK)
	end

	local uri = ngx.var.uri
	local method = ngx.var.request_method

	if method == 'GET' then
		local uid = ngx.var.arg_userid
		if not uid then
			return ngx.exit(ngx.HTTP_OK)	-- 返回到nginx中继续
		end
		if ngx.re.find(uri, [[^.*?server.php$]], "jo") then		-- 获取服务器列表
			-- 获取服务器列表接口
			if _ANALYZE:fetch(uid) then
				ngx.header["Content-Type"] = 'application/json';
				ngx.status = 200            -- 下面的exit直接结束http请求
			end
			return ngx.exit(ngx.HTTP_OK)
		end
	elseif method == 'POST' then
		if ngx.re.find(s, [[^.*?server.php$]], "jo") then		-- 用户创角色接口, 删除用户角色缓存

			local uid = ngx.var.arg_userid
			if not uid then
				return ngx.exit(ngx.HTTP_OK)	-- 返回到nginx中继续
			end

			local timestamp = ngx.var.arg_timestamp
			local sign = ngx.var.arg_sign
			_ANALYZE.delete(uid)
			return ngx.exit(ngx.HTTP_OK)	-- 返回到nginx中继续
		end
		if ngx.re.find(s, [[^.*?server.php$]], "jo") then			-- 服务器列表修改/新增, 删除服务器列表缓存
			ngx.timer.at(0, driver.fetchservers, driver, true)	-- 异步, timer在请求结束以后也继续执行
			return ngx.exit(ngx.HTTP_OK) 	 						-- 返回到nginx中继续
		end
	end
	return ngx.exit(ngx.HTTP_OK)        	 						-- 返回到nginx中继续
end


return _ANALYZE
