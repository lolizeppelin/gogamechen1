from simpleutil.config import cfg
def list_server_opts():
    from simpleservice.ormdb.config import database_opts
    from goperation.manager.wsgi.config import route_opts
    cfg.set_defaults(route_opts, routes=['gogamechen1.api.wsgi.routers'])
    return route_opts + database_opts


def list_agent_opts():
    from gogamechen1 import common
    group = cfg.OptGroup(common.NAME)
    CONF = cfg.CONF
    CONF.register_group(group)
    from goperation.manager.rpc.agent.config import rpc_endpoint_opts
    cfg.set_defaults(rpc_endpoint_opts, module='gogamechen1.api.rpc')
    from gogamechen1.api.rpc.config import register_opts



    return rpc_endpoint_opts
