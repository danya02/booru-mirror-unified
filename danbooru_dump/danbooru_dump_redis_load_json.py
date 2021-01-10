import os
import json
import redis
import time

info = redis.Redis(db=12)

time_start = time.time()
count = 0

os.chdir('/hugedata/booru_old/danbooru2020/danbooru2020/metadata')
for i in os.popen('find -type f'):
    i = i.strip()
    with open(i) as handle:
        for row in handle:
            count += 1
            if count%100 == 0:
                print(count, 'els processed,', count / (time.time() - time_start), 'per second,', info.dbsize(), 'existing')
            if count < info.dbsize():
                continue
            row = row.strip()
            id = json.loads(row)['id']
            info.set(id, row)
