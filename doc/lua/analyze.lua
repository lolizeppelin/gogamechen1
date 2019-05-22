-- 数据解析
local driver = require "driver"
local ngx = require "ngx"


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
			        -- 解析ngx.var.args,不要多次访问ngx.var
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
		return ngx.exit(ngx.OK)
	end
	local switch = _ANALYZE.switch[ngx.var.uri]
	if not switch then
		return ngx.exit(ngx.OK)
	end
	local action = switch(ngx.var.request_method)

	if action == 'get-servers' then
		local servers = driver:getservers()
		if servers then							-- 直接返回缓存数据,不再继续nginx流程
			ngx.header["Content-Type"] = 'application/json';
			ngx.say(servers)
			action = nil
			return ngx.exit(ngx.HTTP_OK)
		end
	elseif action == 'get-roles' then
		local uid = ngx.var.arg_userid
		if uid then
			local roles = driver:getrole(uid)
			if roles then						-- 直接返回缓存数据,不再继续nginx流程
				ngx.header["Content-Type"] = 'application/json';
				ngx.say(roles)
				action = nil
				return ngx.exit(ngx.HTTP_OK)
			end
		end
	end
	if action then
		ngx.ctx.action = action
	end
	return ngx.exit(ngx.OK)
end


function _ANALYZE:body_filter()

	local action = ngx.ctx.action

	if not action or ngx.status ~= 200 then
		return
	end

	local chunk, eof = ngx.arg[1], ngx.arg[2]

	local buffer = ngx.ctx.buffer
	if not buffer then
		buffer = {}
		ngx.ctx.buffer = {}
	end

	if chunk ~= '' then
		buffer[#chunk + 1] = chunk
		ngx.arg[1] = nil
	end

	if eof then
		local raw = table.concat(buffer)
		ngx.ctx.buffer = nil
		if action == 'get-servers' then
			driver:setservers(raw)		-- get servers 接口没有缓存
		elseif action == 'get-roles' then		-- get roles   接口没有缓存
			ngx.timer.at(0, driver.setrole, driver, ngx.ctx.uid, raw)		-- 异步
		elseif action == 'add-role' then
			ngx.timer.at(0, driver.addrole, driver, ngx.ctx.uid, raw)		-- 异步
		elseif action == 'edit-role' then
			ngx.timer.at(0, driver.editrole, driver, ngx.ctx.uid, raw) 	-- 异步
		elseif action == 'edit-server' then
			driver:setservers(raw, true)
		end
	end

end


return _ANALYZE
