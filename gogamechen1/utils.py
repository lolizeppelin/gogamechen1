import re
import psutil

from gogamechen1 import common

regx = re.compile('^[a-z][a-z0-9]+$', re.IGNORECASE)


def find_process(procnames=None):
    if isinstance(procnames, basestring):
        procnames = [procnames, ]
    procnames = procnames or common.ALLTYPES
    pids = []
    for proc in psutil.process_iter(attrs=['pid', 'exe', 'cmdline', 'username', 'cwd']):
        info = proc.info
        if info.get('exe') in procnames:
            pids.append(dict(pid=info.get('pid'),
                             exe=info.get('exe'),
                             # cmdline=[cmd for cmd in info.get('cmdline')],
                             pwd=info.get('cwd'),
                             username=info.get('username')))
    return pids


def validate_string(value, lower=True):
    if not value:
        raise ValueError('String is empty')
    if not isinstance(value, basestring):
        raise ValueError('String is not basestring')
    if not re.match(regx, value):
        raise ValueError('String %s not match regx' % value)
    return value.lower() if lower else value