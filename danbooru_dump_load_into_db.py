from booru_db import BooruDatabase
from database import *
import datetime
import json
import traceback
import random
import time
import logging

os.chdir('/hugedata/booru/danbooru2019/danbooru2019/metadata/small')
files = os.popen('find -type f')

class DanbooruDumpRow(MyModel):
    post_id = IntegerField(unique=True)
    content = TextField()

db.create_tables([DanbooruDumpRow])

try:
    for file in files:
        print('recv file', file)
        if 'orig/' in file:
            print('original: ', orig)
            continue
        file = file.strip()
        if random.random()>=1:
            print('skip', file)
            continue
        to_insert = []
        when = time.time()
        with open(file) as handle:
            for num, line in enumerate(handle):
                js = json.loads(line)
                to_insert.append( {'post_id': js['id'], 'content': line} )
        print('reading file took', time.time()-when, 'seconds')
        try:
            DanbooruDumpRow.insert_many(to_insert).execute()
            print('inserting took', time.time()-when, 'seconds')
            os.unlink(file)
        except IntegrityError:
            print('error while inserting, skipping deletion')
except:
    traceback.print_exc()
    print('err at', file)
