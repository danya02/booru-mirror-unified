import sys
sys.path.append('..')
from database import *
import sys
import os
import redis
import time
r = redis.Redis()

def find_transfer():
    f = os.popen('find /hugedata/booru/danbooru_temp -type f')
    for row in f:
        row = row.strip()
        if row.split('/')[-1].startswith('.'): continue
        print(row, file=sys.stderr)
        r.set(row, '0')
if __name__ == '__main__':
    while 1:
        find_transfer()
        time.sleep(20)
