#流程图

---

![avatar](flow.png)

---

nginx 全局配置文件

```text
http {
    lua_package_path   "/usr/share/doc/python-gogamechen1-1.0.0/lua/?.lua;/usr/lib64/openresty/lua/5.1/?.lua;";
    lua_package_cpath  "/usr/lib64/openresty/lua/5.1/?.so;";
    lua_shared_dict gogamechen1 128k;
    lua_shared_dict gogameroles 512m;

    init_by_lua_block {
        require "resty.core"
    	require 'resty.core.regex'
        require 'resty.core.ctx'
    }
    
    ...
}

```

站点配置

```text
server {
    
    ...
    
    location ~ \.(php)$ {
        
        access_by_lua_block {
            package.path = '/etc/nginx/lua/?.lua;' .. package.path 
            local analyze = require "analyze"
            local config = require "config"
            analyze:access_filter(config)
        }
        
        body_filter_by_lua_block {
            package.path = '/etc/nginx/lua/?.lua' .. package.path 
            local analyze = require "analyze"
            analyze:body_filter()
        }
        ...
    }
    
    ...
}


```