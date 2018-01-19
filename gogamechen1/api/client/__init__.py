from simpleservice.plugin.exceptions import ServerExecuteRequestError

from gopdb.api.client import GopDBClient
from gopcdn.api.client import GopCdnClient

from goperation.manager import common


class GogameChen1DBClient(GopDBClient, GopCdnClient):

    objfiles_path = '/gogamechen1/objfiles'
    objfile_path = '/gogamechen1/objfiles/%s'
    objfile_path_ex = '/gogamechen1/objfiles/%s/%s'

    groups_path = '/gogamechen1/groups'
    group_path = '/gogamechen1/groups/%s'
    group_path_ex = '/gogamechen1/groups/%s/%s'

    games_path = '/gogamechen1/group/%s/gamesvr/entitys'
    game_path = '/gogamechen1/group/%s/gamesvr/entitys/%s'

    gms_path = '/gogamechen1/group/%s/loginsvr/entitys'
    gm_path = '/gogamechen1/group/%s/loginsvr/entitys/%s'

    crosss_path = '/gogamechen1/group/%s/publicsvr/entitys'
    cross_path = '/gogamechen1/group/%s/publicsvr/entitys/%s'

    bond_path = '/gogamechen1/entity/%s'
    appentitys_path = '/gogamechen1/entitys'
    reset_path = '/gogamechen1/group/%s/%s/entitys/%s/reset'

    all_packages_path = '/gogamechen1/packages'
    packages_path = '/gogamechen1/group/%s/packages'
    package_path = '/gogamechen1/group/%s/packages/%s'

    packagefiles_path = '/gogamechen1/package/%s/pfiles'
    packagefile_path = '/gogamechen1/package/%s/pfiles/%s'

    def objfiles_index(self, body=None):
        resp, results = self.get(action=self.objfiles_path, body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list gogamechen1 objfiles fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def objfile_create(self, objtype, subtype, version, fileinfo, body=None):
        body = body or {}
        body.setdefault('objtype', objtype)
        body.setdefault('subtype', subtype)
        body.setdefault('version', version)
        body.setdefault('fileinfo', fileinfo)
        resp, results = self.retryable_post(action=self.objfiles_path,
                                            body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='create gogamechen1 objfiles fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def objfile_show(self, uuid):
        resp, results = self.get(action=self.objfile_path % uuid, body=None)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='show gogamechen1 objfiles fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def objfile_update(self, uuid):
        raise NotImplementedError

    def objfile_delete(self, uuid):
        resp, results = self.delete(action=self.objfile_path % uuid)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='delete gogamechen1 objfiles fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def objfile_send(self, uuid, objtype, body=None):
        resp, results = self.retryable_post(action=self.objfile_path_ex % (uuid, 'send'), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='send gogamechen1 objfiles fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    # -----------group api-----------------
    def groups_index(self, body=None):
        resp, results = self.get(action=self.groups_path, body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list gogamechen1 group fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def groups_create(self, name, desc=None):
        resp, results = self.post(action=self.groups_path, body=dict(name=name, desc=desc))
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='create gogamechen1 group fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def group_show(self, group_id, detail=False):
        resp, results = self.get(action=self.group_path % str(group_id), body=dict(detail=detail))
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='show gogamechen1 group fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def group_update(self, group_id, body=None):
        raise NotImplementedError

    def group_delete(self, group_id, body=None):
        resp, results = self.delete(action=self.group_path % str(group_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='delete gogamechen1 group fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def group_maps(self, group_id, body=None):
        resp, results = self.get(action=self.group_path_ex % (str(group_id), 'maps'), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='get gogamechen1 group maps fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def group_chiefs(self, group_id, body=None):
        resp, results = self.get(action=self.group_path_ex % (str(group_id), 'chiefs'), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='get gogamechen1 group chiefs fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    # -----------bond database api-----------------
    def bondto(self, entity, databases):
        resp, results = self.post(action=self.bond_path % str(entity), body=dict(databases=databases),
                                  timeout=15)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='bond gogamechen1 databases fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentitys(self, entitys):
        resp, results = self.get(action=self.appentitys_path, body=dict(entitys=entitys),
                                 timeout=15)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='get gogamechen1 entitys fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def reset(self,  group_id, objtype, entity, body=None):
        resp, results = self.post(action=self.reset_path % (str(group_id), objtype, str(entity)), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='reset gogamechen1 %s.%d fail:%d' % (results['resultcode'],
                                                                                         objtype, entity),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    # -----------game server api-----------------
    def games_index(self, group_id, body=None):
        resp, results = self.get(action=self.games_path % str(group_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list gogamechen1 gameserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def games_create(self, group_id, body=None):
        resp, results = self.post(action=self.games_path % str(group_id), body=body, timeout=15)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='create gogamechen1 gameserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def game_show(self, group_id, entity, detail=False):
        resp, results = self.get(action=self.game_path % (str(group_id), str(entity)),
                                 body=dict(detail=detail))
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='show gogamechen1 gameserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def game_update(self, group_id, entity, detail=False):
        raise NotImplementedError

    def game_delete(self, group_id, entity, clean='unquote'):
        resp, results = self.delete(action=self.game_path % (str(group_id), str(entity)),
                                    body=dict(clean=clean))
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='delete gogamechen1 gameserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    # -----------gm server api-----------------
    def gms_index(self, group_id, body=None):
        resp, results = self.get(action=self.gms_path % str(group_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list gogamechen1 gmserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def gms_create(self, group_id, body=None):
        resp, results = self.post(action=self.gms_path % str(group_id), body=body, timeout=15)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='create gogamechen1 gmserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def gm_show(self, group_id, entity, detail=False):
        resp, results = self.get(action=self.gm_path % (str(group_id), str(entity)),
                                 body=dict(detail=detail))
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='show gogamechen1 gmserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def gm_update(self, group_id, entity, detail=False):
        raise NotImplementedError

    def gm_delete(self, group_id, entity, clean='unquote'):
        resp, results = self.delete(action=self.gm_path % (str(group_id), str(entity)),
                                    body=dict(clean=clean))
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='delete gogamechen1 gmserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    # -----------cross server api-----------------
    def crosss_index(self, group_id, body=None):
        resp, results = self.get(action=self.crosss_path % str(group_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list gogamechen1 gmserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def crosss_create(self, group_id, body=None):
        resp, results = self.post(action=self.crosss_path % str(group_id), body=body, timeout=15)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='create gogamechen1 gmserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def cross_show(self, group_id, entity, detail=False):
        resp, results = self.get(action=self.cross_path % (str(group_id), str(entity)),
                                 body=dict(detail=detail))
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='show gogamechen1 gmserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def cross_update(self, group_id, entity, detail=False):
        raise NotImplementedError

    def cross_delete(self, group_id, entity, clean='unquote'):
        resp, results = self.delete(action=self.cross_path % (str(group_id), str(entity)),
                                    body=dict(clean=clean))
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='delete gogamechen1 gmserver fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    # -----------package api-----------------
    def package_all(self):
        resp, results = self.get(action=self.all_packages_path)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list all package fail',
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def package_create(self, group_id, resource_id, package_name, mark,
                       body=None):
        body = body or {}
        body.update({'resource_id': resource_id, 'group_id': group_id,
                     'package_name': package_name, 'mark': mark})
        resp, results = self.post(action=self.packages_path % str(group_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='create %d package fail:%d' %
                                                    (group_id, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def package_index(self, group_id, body=None):
        resp, results = self.get(action=self.packages_path % str(group_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list %d package fail:%d' %
                                                    (group_id, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def package_show(self, group_id, package_id, body):
        resp, results = self.get(action=self.package_path % (str(group_id), package_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='show %d package fail:%d' %
                                                    (group_id, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def package_update(self, group_id, package_id, body):
        resp, results = self.put(action=self.package_path % (str(group_id), package_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='update %d package fail:%d' %
                                                    (group_id, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def package_delete(self, group_id, package_id, body):
        resp, results = self.delete(action=self.package_path % (str(group_id), package_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='delete %d package fail:%d' %
                                                    (group_id, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    # -----------package file api-----------------

    def packagefile_create(self, package_id, body):
        resp, results = self.post(action=self.packagefiles_path % str(package_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='create %d package file fail:%d' %
                                                    (package_id, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def packagefile_index(self, package_id, body):
        resp, results = self.get(action=self.packagefiles_path % str(package_id), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list %d package file fail:%d' %
                                                    (package_id, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def packagefile_show(self, package_id, pfile_id, body):
        resp, results = self.get(action=self.package_path % (str(package_id), str(pfile_id)), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='show %d package file %d fail:%d' %
                                                    (package_id, pfile_id, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def packagefile_update(self, package_id, pfile_id, body):
        resp, results = self.put(action=self.package_path % (str(package_id), str(pfile_id)), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='update %d package file %d fail:%d' %
                                                    (package_id, pfile_id, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def packagefile_delete(self, package_id, pfile_id, body):
        resp, results = self.delete(action=self.package_path % (str(package_id), str(pfile_id)), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='delete %d package file %d fail:%d' %
                                                    (package_id, pfile_id, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results
