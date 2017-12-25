import psutil

from gogamechen1 import common

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
