from simpleservice.wsgi import router
from simpleservice.wsgi.middleware import controller_return_response

from gogamechen1 import common
from gogamechen1.api.wsgi import controller


COLLECTION_ACTIONS = ['index', 'create']
MEMBER_ACTIONS = ['show', 'update', 'delete']


class Routers(router.RoutersBase):

    def append_routers(self, mapper, routers=None):


        resource_name = 'objfile'
        collection_name = resource_name + 's'

        group_controller = controller_return_response(controller.ObjtypeFileReuest(),
                                                   controller.FAULT_MAP)
        mapper.collection(collection_name=collection_name,
                          resource_name=resource_name,
                          controller=group_controller,
                          path_prefix='/%s' % common.NAME,
                          member_prefix='/{uuid}',
                          collection_actions=COLLECTION_ACTIONS,
                          member_actions=MEMBER_ACTIONS)


        resource_name = 'group'
        collection_name = resource_name + 's'

        group_controller = controller_return_response(controller.GroupReuest(),
                                                   controller.FAULT_MAP)
        mapper.collection(collection_name=collection_name,
                          resource_name=resource_name,
                          controller=group_controller,
                          path_prefix='/%s' % common.NAME,
                          member_prefix='/{group_id}',
                          collection_actions=COLLECTION_ACTIONS,
                          member_actions=MEMBER_ACTIONS)

        resource_name = 'entity'
        collection_name = resource_name + 's'

        game_controller = controller_return_response(controller.AppEntityReuest(),
                                                   controller.FAULT_MAP)

        self._add_resource(mapper, game_controller,
                           path='/%s/entity/{entity}' % common.NAME,
                           post_action='bondto')

        self._add_resource(mapper, game_controller,
                   path='/%s/chiefs' % common.NAME,
                   get_action='chiefs')

        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=game_controller,
                                       path_prefix='/%s/group/{group_id}/{objtype}' % common.NAME,
                                       member_prefix='/{entity}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)
        # collection.member.link('databases', method='GET')
        # collection.member.link('stop', method='POST')
        # collection.member.link('status', method='POST')
        # collection.member.link('hotfix', method='POST')
        # collection.member.link('upgrade', method='POST')

