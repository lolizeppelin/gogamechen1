from gogamechen1.cmd.db.utils import init_gopdb

dst = {'host': '172.20.0.3',
       'port': 3304,
       'schema': 'gogamechen1',
       'user': 'root',
       'passwd': '111111'}

init_gopdb(dst)