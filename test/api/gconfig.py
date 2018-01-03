import re


files = ['bin/', 'bin/libbehaviac.so', 'bin/gamesvr', 'bin/libgointerface.so',
         'behaviac/loli/',
         'config/', 'config/1.json',
         'bin/logsvr'
         ]

regx = re.compile('^bin/(libbehaviac.so|libgointerface.so|gamesvr)*?$|'
                  '^behaviac/([\S]+?/|[\S]+?\.xml)*?$|'
                  '^(config|geology)/([\S]+?\.json)*?$')


for name in files:
    ret  = re.match(regx, name)
    if ret:
        print ret.group(0), name
    else:
        print 'not found~~~~~', name
