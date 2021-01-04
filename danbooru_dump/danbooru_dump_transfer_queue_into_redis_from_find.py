import sys
sys.path.append('..')
from database import *
import sys
import os
import redis
r = redis.Redis()

f = os.popen('find /hugedata/booru_old/danbooru2019/danbooru2019/original -type f')
for row in f:
    row = row.strip()
    print(row, file=sys.stderr)
    r.set(row, '0')
