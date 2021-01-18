import os
import json
import redis
import time

info = redis.Redis(db=12)

time_start = time.time()
count = 0

os.chdir('/hugedata/booru_old/danbooru2020/danbooru2020/metadata/split')
for i in sorted(os.listdir('.')):
    while info.dbsize() > 100000:
        print('too many files', info.dbsize())
        time.sleep(60)
    i = i.strip()
    print('read', i)
    with open(i) as handle:
        for row in handle:
            row = row.strip()
            id = json.loads(row)['id']
            info.set(id, row)
    print('delete', i)
    os.unlink(i)

