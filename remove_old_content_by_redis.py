from database import *
import redis
import time

r = redis.Redis(db=2)

while 1:
    if r.dbsize() > 20:
        ids = []
        for _ in range(100):
            id = int(r.randomkey())
            ids.append(id)
        ids = list(set(ids))
        print('deleting', ids)
        ContentOld.delete().where(ContentOld.id.in_(ids)).execute()
        for i in ids:
            r.delete(i)
        print('deleted', ids)
    else:
        print('db not enough!', r.dbsize())
        time.sleep(30)
