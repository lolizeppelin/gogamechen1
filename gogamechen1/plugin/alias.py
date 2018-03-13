import os
from gogamechen1 import common
from gopcdn.plugin.alias import BaseCdnAlias


class Alias(BaseCdnAlias):
    def alias(self, path, version):
        path = os.path.join(path, 'version.txt')
        if not os.path.exists(path) or not os.path.isfile(path) or os.path.getsize(path) > 100:
            raise ValueError('version file over size')
        with open(path, 'r') as f:
            buffer = f.read()
            return buffer.strip()

    def _endpoint_name(self):
        return common.NAME
