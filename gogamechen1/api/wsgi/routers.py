from simpleservice.wsgi import router
from simpleservice.wsgi.middleware import controller_return_response

from gogamechen1 import common
from gogamechen1.api.wsgi import game
from gogamechen1.api.wsgi import resource


COLLECTION_ACTIONS = ['index', 'create']
MEMBER_ACTIONS = ['show', 'update', 'delete']


class Routers(router.RoutersBase):

    resource_name = 'gogamechen1'

    def append_routers(self, mapper, routers=None):

        resource_name = 'objfile'
        collection_name = resource_name + 's'

        objfile_controller = controller_return_response(resource.ObjtypeFileReuest(),
                                                        resource.FAULT_MAP)
        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=objfile_controller,
                                       path_prefix='/%s' % common.NAME,
                                       member_prefix='/{md5}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)
        collection.member.link('send', method='PUT')

        resource_name = 'package'
        collection_name = resource_name + 's'
        package_controller = controller_return_response(resource.PackageReuest(),
                                                        resource.FAULT_MAP)
        self._add_resource(mapper, package_controller,
                           path='/%s/packages' % common.NAME,
                           get_action='packages')

        self._add_resource(mapper, package_controller,
                           path='/%s/group/{group_id}/resources' % common.NAME,
                           get_action='resources')

        self._add_resource(mapper, package_controller,
                           path='/%s/group/{group_id}/resources/{resource_id}' % common.NAME,
                           put_action='updates')

        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=package_controller,
                                       path_prefix='/%s/group/{group_id}' % common.NAME,
                                       member_prefix='/{package_id}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)
        collection.member.link('upgrade', method='PUT')
        # add cdn resource file
        collection.member.link('remark', name='add_remark', method='POST', action='add_remark')
        # delete cdn resource file
        collection.member.link('remark', name='del_remark', method='DELETE', action='del_remark')
        # list cdn resource file
        collection.member.link('remark', name='list_remarks', method='GET', action='list_remarks')

        resource_name = 'pfile'
        collection_name = resource_name + 's'
        pfile_controller = controller_return_response(resource.PackageFileReuest(),
                                                      resource.FAULT_MAP)

        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=pfile_controller,
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
        collection.member.link('areas', method='GET')
        collection.member.link('packages', method='GET')
        collection.member.link('area', method='PUT')

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

        self._add_resource(mapper, game_controller,
                           path='/%s/{objtype}/databases' % common.NAME,
                           get_action='databases')

        self._add_resource(mapper, game_controller,
                           path='/%s/{objtype}/agents' % common.NAME,
                           get_action='agents')

        self._add_resource(mapper, game_controller,
                           path='/%s/merge' % common.NAME,
                           post_action='merge')

        self._add_resource(mapper, game_controller,
                           path='/%s/merge/{uuid}' % common.NAME,
                           put_action='continues')

        self._add_resource(mapper, game_controller,
                           path='/%s/finish/{uuid}' % common.NAME,
                           put_action='finish')

        self._add_resource(mapper, game_controller,
                           path='/%s/mergeing/{entity}/swallow' % common.NAME,
                           post_action='swallow')

        self._add_resource(mapper, game_controller,
                           path='/%s/mergeing/{entity}/swallowed' % common.NAME,
                           post_action='swallowed')

        collection = mapper.collection(collection_name=collection_name,
                                       resource_name=resource_name,
                                       controller=game_controller,
                                       path_prefix='/%s/group/{group_id}/{objtype}' % common.NAME,
                                       member_prefix='/{entity}',
                                       collection_actions=COLLECTION_ACTIONS,
                                       member_actions=MEMBER_ACTIONS)
        collection.member.link('quote', name='quote_version', method='PUT', action='quote_version')
        collection.member.link('quote', name='unquote_version', method='DELETE', action='unquote_version')
        collection.member.link('clean', method='DELETE')
        collection.member.link('reset', method='POST')
        collection.member.link('migrate', method='POST')
        collection.member.link('start', method='POST')
        collection.member.link('stop', method='POST')
        collection.member.link('status', method='GET')
        collection.member.link('opentime', method='PUT')
        collection.member.link('upgrade', method='POST')
        collection.member.link('flushconfig', method='PUT')
        collection.member.link('hotfix', method='POST')
