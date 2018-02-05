import time
from simpleservice.plugin.exceptions import ServerExecuteRequestError

from gopdb.api.client import GopDBClient
from gopcdn.api.client import GopCdnClient

from goperation.manager import common

from gogamechen1.common import GAMESERVER


class GogameChen1DBClient(GopDBClient, GopCdnClient):

    objfiles_path = '/gogamechen1/objfiles'
    objfile_path = '/gogamechen1/objfiles/%s'
    objfile_path_ex = '/gogamechen1/objfiles/%s/%s'

    groups_path = '/gogamechen1/groups'
    group_path = '/gogamechen1/groups/%s'
    group_path_ex = '/gogamechen1/groups/%s/%s'

    agents_chioces_path = '/gogamechen1/%s/agents'
    databases_chioces_path = '/gogamechen1/%s/databases'

    appentitys_path = '/gogamechen1/group/%s/%s/entitys'
    appentity_path = '/gogamechen1/group/%s/%s/entitys/%s'
    appentity_path_ex = '/gogamechen1/group/%s/%s/entitys/%s/%s'

    bond_path = '/gogamechen1/entity/%s'
    appentitys_all_path = '/gogamechen1/entitys'
    reset_path = '/gogamechen1/group/%s/%s/entitys/%s/reset'

    all_packages_path = '/gogamechen1/packages'
    all_resources_path = '/gogamechen1/resources'
    packages_path = '/gogamechen1/group/%s/packages'
    package_path = '/gogamechen1/group/%s/packages/%s'
    package_path_ex = '/gogamechen1/group/%s/packages/%s/%s'

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

    def group_areas(self, group_id, body=None):
        resp, results = self.get(action=self.group_path_ex % (str(group_id), 'areas'), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='get gogamechen1 group areas fail:%d' % results['resultcode'],
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
        resp, results = self.get(action=self.appentitys_all_path, body=dict(entitys=entitys),
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

    # -----------chioces api-----------------
    def agents_chioces(self, objtype, zone=None):
        body = None
        if zone:
            body = dict(zone=zone)
        resp, results = self.get(action=self.agents_chioces_path % objtype, body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list agents chioces fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def databases_chioces(self, objtype, zone=None):
        body = None
        if zone:
            body = dict(zone=zone)
        resp, results = self.get(action=self.databases_chioces_path % objtype, body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list databases chioces fail:%d' % results['resultcode'],
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    # -----------appentity api-----------------
    def appentitys_index(self, group_id, objtype, body=None):
        resp, results = self.get(action=self.appentitys_path % (str(group_id), objtype), body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentitys_create(self, group_id, objtype, body=None):
        resp, results = self.post(action=self.appentitys_path % (str(group_id), objtype), body=body,
                                  timeout=30)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='create %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentity_show(self, group_id, objtype, entity, body=None):
        resp, results = self.get(action=self.appentity_path % (str(group_id), objtype, str(entity)),
                                 body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='show %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentity_update(self, group_id, objtype, entity, body):
        resp, results = self.put(action=self.appentity_path % (str(group_id), objtype, str(entity)),
                                 body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='update %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentity_delete(self, group_id, objtype, entity, body=None):
        resp, results = self.delete(action=self.appentity_path % (str(group_id), objtype, str(entity)),
                                    body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='delete %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentity_clean(self, group_id, objtype, entity, delete=False):
        action = 'delete' if delete else 'unquote'
        resp, results = self.delete(action=self.appentity_path_ex % (str(group_id), objtype,
                                                                     str(entity), 'clean'),
                                    body=dict(clean=action))
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='clean %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentity_start(self, group_id, objtype, entity, body=None):
        resp, results = self.post(action=self.appentity_path_ex % (str(group_id), objtype, str(entity), 'start'),
                                  body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='start %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentity_stop(self, group_id, objtype, entity, body=None):
        resp, results = self.post(action=self.appentity_path_ex % (str(group_id), objtype, str(entity), 'start'),
                                  body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='stop %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentity_status(self, group_id, objtype, entity, body=None):
        resp, results = self.get(action=self.appentity_path_ex % (str(group_id), objtype, str(entity), 'status'),
                                 body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='status %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentity_upgrade(self, group_id, objtype, entity, body=None):
        resp, results = self.post(action=self.appentity_path_ex % (str(group_id), objtype, str(entity), 'upgrade'),
                                  body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='upgrade %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentity_flushconfig(self, group_id, objtype, entity, body=None):
        resp, results = self.post(action=self.appentity_path_ex % (str(group_id), objtype,
                                                                   str(entity), 'flushconfig'),
                                  body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='flushconfig %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentity_reset(self, group_id, objtype, entity, body=None):
        resp, results = self.post(action=self.appentity_path_ex % (str(group_id), objtype,
                                                                   str(entity), 'reset'),
                                  body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='reset %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def appentity_hotfix(self, group_id, objtype, entity, body=None):
        resp, results = self.post(action=self.appentity_path_ex % (str(group_id), objtype,
                                                                   str(entity), 'hotfix'),
                                  body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='hotfix %s fail:%d' % (objtype, results['resultcode']),
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def game_opentime(self, group_id, entity, opentime):
        resp, results = self.put(action=self.appentity_path_ex % (str(group_id), GAMESERVER,
                                                                  str(entity), 'opentime'),
                                 body=dict(opentime=opentime))
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='reset %s fail:%d' % (GAMESERVER, results['resultcode']),
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

    def package_resource_all(self):
        resp, results = self.get(action=self.all_resources_path)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='list all package resource fail',
                                            code=resp.status_code,
                                            resone=results['result'])
        return results

    def package_resource_upgrade(self, group_id, package_id, version, timeout=60):
        timeout = timeout or 60
        body = dict(version=version, request_time=int(time.time()),
                    finishtime=int(time.time() + timeout))
        resp, results = self.put(action=self.package_path_ex % (str(group_id), package_id, 'upgrade'),
                                 body=body)
        if results['resultcode'] != common.RESULT_SUCCESS:
            raise ServerExecuteRequestError(message='upgrade %d package resource fail:%d' %
                                                    (group_id, results['resultcode']),
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

    def package_show(self, group_id, package_id, body=None):
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

    def package_delete(self, group_id, package_id, body=None):
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
