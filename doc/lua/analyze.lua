-- 数据解析
local driver = require "driver"


local _ANALYZE = {
	switch = ngx.null,
}


function _ANALYZE:init(config)

	if _ANALYZE.switch ~= ngx.null then
		return true, nil
	end

	-- TODO check urlpath
	local urlpath = config.urlpath or ''

	-- init driver
	local ok, err = driver:init(config)
	if not ok then
		return nil, 'Init lua cache driver fail:' .. err
	end

	--usrid = 0 _123456789 & timestamp = 1557373019 & sign = MD5(usrid + "asldajldl" + timestamp)

	local switch = {
		[urlpath .. '/server.php'] = function (method)
			if method == 'GET' then
				return 'get-servers'
			end
		end,
		[urlpath .. '/users.php'] = function (method)
			if method == 'GET'then
				return 'get-roles'
			end
		end,
		[urlpath .. '/nuser.php'] = function (method)
			if method == 'POST'then
				return 'add-role'
			end
		end,
		[urlpath .. '/euser.php'] = function (method)
			if method == 'POST' then
				return 'edit-role'
			end
		end,
		[urlpath .. '/index.php'] = function (method)
			if method == 'POST'
					and ngx.var.arg_m == 'Admin'
					and ngx.var.arg_c == 'Operation'
					and ngx.var.arg_a == 'server_batch_set' then
				return 'edit-server'
			end
		end,
	}
	_ANALYZE.switch = switch
	return true, nil

end


function _ANALYZE:access_filter(config)

	local ok, err = _ANALYZE:init(config)
	if not ok then
		ngx.log('Init lua analyze fail:' .. err)
		return ngx.exit(ngx.HTTP_OK)
	end

	local switch = _ANALYZE.switch[ngx.var.uri]
	if not switch then
		return ngx.exit(ngx.HTTP_OK)
	end
	local action = switch(ngx.var.request_method)
	if not action then
		return ngx.exit(ngx.HTTP_OK)
	end

	ngx.ctx.action = action

	if action == 'get-servers' then
		local servers = driver:getservers()
		if servers then							-- 直接返回缓存数据,不再继续nginx流程
			ngx.header["Content-Type"] = 'application/json';
			table.remove(ngx.ctx, 'action')
			ngx.say(servers)
			ngx.status = 200
		end
	elseif action == 'get-roles' then
		local uid = ngx.var.arg_userid
		if uid then
			local roles = driver:getrole(uid)
			if roles then						-- 直接返回缓存数据,不再继续nginx流程
				table.remove(ngx.ctx, 'action')
				ngx.header["Content-Type"] = 'application/json';
				ngx.say(roles)
				ngx.status = 200
			end
		end
	end
	return ngx.exit(ngx.HTTP_OK)		-- 未设置ngx.status的情况下返回nginx继续
end


function _ANALYZE:body_filter()

	if not ngx.ctx.action or ngx.status ~= 200 then
		return ngx.exit(ngx.HTTP_OK)
	end

	local action = ngx.ctx.action

	if action == 'get-servers' then
		driver:setservers(ngx.arg[1])		-- get servers 接口没有缓存
	elseif action == 'get-roles' then		-- get roles   接口没有缓存
		ngx.timer.at(0, driver.setrole, driver, ngx.ctx.uid, ngx.arg[1])		-- 异步
	elseif action == 'add-role' then
		ngx.timer.at(0, driver.addrole, driver, ngx.ctx.uid, ngx.arg[1])		-- 异步
	elseif action == 'edit-role' then
		ngx.timer.at(0, driver.editrole, driver, ngx.ctx.uid, ngx.arg[1]) 	-- 异步
	elseif action == 'edit-server' then
		driver:setservers(ngx.arg[1], true)
	end

	return ngx.exit(ngx.HTTP_OK)
end

return _ANALYZE
