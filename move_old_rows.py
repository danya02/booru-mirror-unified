from database import *
import os
import redis

r = redis.Redis(db=2)
os.chdir('/hugedata/booru')

for row in ContentOld.select().where(ContentOld.mimetype_id==2).limit(100000).iterator():
    Content.get_or_create(post_id=row.post_id, sha256_current=row.sha256_current, mimetype_id=row.mimetype_id, file_size_current=row.file_size_current)
    ensure_dir(row.sha256_current)
    try:
        os.rename('old/'+get_path(row.sha256_current), get_path(row.sha256_current)+'.'+row.mimetype.ext)
        r.set(row.id, 0)
    except:
        continue
