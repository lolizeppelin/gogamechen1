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
        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=group_controller,
                                       path_prefix='/%s' % common.NAME,
                                       member_prefix='/{uuid}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)
        # collection.member.link('send', method='POST')


        resource_name = 'group'
        collection_name = resource_name + 's'

        group_controller = controller_return_response(controller.GroupReuest(),
                                                   controller.FAULT_MAP)
        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=group_controller,
                                       path_prefix='/%s' % common.NAME,
                                       member_prefix='/{group_id}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)
        collection.member.link('maps', method='GET')
        collection.member.link('chiefs', method='GET')
        collection.member.link('start', method='POST')
        collection.member.link('stop', method='POST')
        collection.member.link('status', method='POST')
        # collection.member.link('hotfix', method='POST')
        # collection.member.link('upgrade', method='POST')

        resource_name = 'entity'
        collection_name = resource_name + 's'

        game_controller = controller_return_response(controller.AppEntityReuest(),
                                                   controller.FAULT_MAP)

        self._add_resource(mapper, game_controller,
                           path='/%s/entity/{entity}' % common.NAME,
                           post_action='bondto')

        self._add_resource(mapper, game_controller,
                   path='/%s/entitys' % common.NAME,
                   get_action='entitys')

        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=game_controller,
                                       path_prefix='/%s/group/{group_id}/{objtype}' % common.NAME,
                                       member_prefix='/{entity}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)
        collection.member.link('reset', method='POST')
        collection.member.link('start', method='POST')
        collection.member.link('stop', method='POST')
        collection.member.link('status', method='POST')
        collection.member.link('opentime', method='PUT')
        # collection.member.link('hotfix', method='POST')
        # collection.member.link('upgrade', method='POST')

        # resource_name = 'package'
        # collection_name = resource_name + 's'
        # collection = mapper.collection(collection_name=collection_name,
        #                                resource_name=resource_name,
        #                                controller=controller_return_response(controller.PackageReuest(),
        #                                                                      controller.FAULT_MAP),
        #                                path_prefix='/%s/group/{group_id}' % common.NAME,
        #                                member_prefix='/{package_id}',
        #                                collection_actions=COLLECTION_ACTIONS,
        #                                member_actions=MEMBER_ACTIONS)
        # collection.member.link('source', name='add_package_source', method='POST', action='add_source')
        # collection.member.link('source', name='del_package_source', method='DELETE', action='delete_source')
        # collection.member.link('source', name='update_package_source', method='PUT', action='update_source')
        # collection.member.link('cdngroup', method='PUT')
