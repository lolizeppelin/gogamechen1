REGEX = '^([A-Za-z]+?[A-Za-z-_0-9]*?[A-Za-z0-9]+?):([a-zA-Z0-9-_.]+?)@tcp\(([a-z0-9-.]+?):([0-9]+?)\)/([a-z]+?[a-z0-9-_]*?[a-z0-9]+?)\?charset=([a-z0-9]+?)$'

uri = 'gogamelog-rw-8:111111@tcp(192.168.1.10:3002)/gogamechen1_gamesvr_logdb_8?charset=utf8'

import re

match = re.match(REGEX, uri)
if match:
    # print match
    print match.groups()
    print match.group(1)


    # aaa = '^([A-Za-z]+?[A-Za-z-_0-9]*?[A-Za-z0-9]+?):([a-zA-Z0-9-_.]+?):@tcp'
    #
    # match = re.match(aaa, 'gogamelog-rw-8:111111@tcp')
    #
    # print match

targets = set([1, 2, 3])


def is_include(entity):
    for area in entity.area:
        if set(area.packages) & targets:
            return True
    return False


ok = [entity for entity in entitys if is_include(entity)]
