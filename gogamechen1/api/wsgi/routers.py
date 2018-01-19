from simpleservice.wsgi import router
from simpleservice.wsgi.middleware import controller_return_response

from gogamechen1 import common
from gogamechen1.api.wsgi import game
from gogamechen1.api.wsgi import resource


COLLECTION_ACTIONS = ['index', 'create']
MEMBER_ACTIONS = ['show', 'update', 'delete']


class Routers(router.RoutersBase):

    def append_routers(self, mapper, routers=None):


        resource_name = 'objfile'
        collection_name = resource_name + 's'

        objfile_controller = controller_return_response(resource.ObjtypeFileReuest(),
                                                        resource.FAULT_MAP)
        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=objfile_controller,
                                       path_prefix='/%s' % common.NAME,
                                       member_prefix='/{uuid}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)
        collection.member.link('send', method='POST')

        resource_name = 'package'
        collection_name = resource_name + 's'
        package_controller = controller_return_response(resource.PackageReuest(),
                                                        resource.FAULT_MAP)
        self._add_resource(mapper, package_controller,
                           path='/%s/packages' % common.NAME,
                           get_action='packages')
        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=package_controller,
                                       path_prefix='/%s/group/{group_id}' % common.NAME,
                                       member_prefix='/{package_id}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)
        # collection.member.link('file', name='add_package_file', method='POST', action='add_file')
        # collection.member.link('file', name='add_package_file', method='DELETE', action='delete_file')

        resource_name = 'pfile'
        collection_name = resource_name + 's'
        package_controller = controller_return_response(resource.PackageFileReuest(),
                                                        resource.FAULT_MAP)
        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=package_controller,
                                       path_prefix='/%s/package/{package_id}' % common.NAME,
                                       member_prefix='/{pfile_id}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)

        resource_name = 'group'
        collection_name = resource_name + 's'
        group_controller = controller_return_response(game.GroupReuest(),
                                                      game.FAULT_MAP)
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

        game_controller = controller_return_response(game.AppEntityReuest(),
                                                     game.FAULT_MAP)

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
        collection.member.link('upgrade', method='POST')
