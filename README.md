# gogamchen1
具体游戏管理模块
后端程序员 小陈
开发语言 go





paste文件修改


路由配置修改
# 启用登陆认证相关路由
routes = goperation.manager.wsgi.login.private
publics = goperation.manager.wsgi.login.public


fernet初始化
# fernet存放位置
fernet_key_repository = /etc/goperation/fernet



前端配置认证地址修改
login: '/n1.0/goperation/login',   /* login path */
loginout: '/v1.0/goperation/login',   /* login path */