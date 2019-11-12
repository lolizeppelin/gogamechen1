#!/usr/bin/python
# -*- encoding: utf-8 -*-
import sys
import logging

from simpleutil.config import cfg

from goperation.api.client import ManagerClient
from goperation.api.client.config import client_opts

from goperation.api.client.utils import prepare_results


from gogamechen1.api.client import GogameChen1DBClient

CONF = cfg.CONF


def client(session=None):
    return GogameChen1DBClient(httpclient=ManagerClient(url=CONF.gcenter, port=CONF.gcenter_port,
                                                        retries=CONF.retries, timeout=CONF.apitimeout,
                                                        token=CONF.trusted, session=session))


def merge(appfile, group, entitys):
    _client = client()
    code, result, data = prepare_results(_client.merge_entitys, {'appfile': appfile,
                                                                 'group_id': group,
                                                                 'entitys': entitys,
                                                                 })
    if code:
        print('\033[1;31;40m')
        print 'Fail, code %d, result %s' % (code, result)
        if data:
            print data
        print('\033[0m')
        sys.exit(1)
    print('merge success')
    print(data)
    print('===========================')


def continue_merge(uuid):
    _client = client()
    code, result, data = prepare_results(_client.continue_merge, uuid)
    if code:
        print('\033[1;31;40m')
        print 'Fail, code %d, result %s' % (code, result)
        if data:
            print data
        print('\033[0m')
        sys.exit(1)
    print('continue success')
    print(data)
    print('===========================')


def main():
    logging.basicConfig(level=logging.WARN)
    CONF.register_cli_opts(client_opts)
    CONF(project='cmd')

    appfile = 'da27850e62bbd8301adfc6189602f659'
    group = 1

    entitys = [27, 28]
    uuid = ''

    merge(appfile, group, entitys)
    #continue_merge(uuid)


if __name__ == '__main__':
    main()
