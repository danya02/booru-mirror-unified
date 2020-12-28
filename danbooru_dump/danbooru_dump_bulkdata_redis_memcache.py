import redis
import time
import sys
sys.path.append('..')
from database import *
import magic

main = redis.Redis()
cache = redis.Redis(db=2)
also_save_to_db = True

while 1:
    size = cache.dbsize()
    print(size)
    if size>1000:
        time.sleep(25)
        continue
    k = str(main.randomkey(), 'utf8')
    with open(k, 'rb') as handle:
        data = handle.read()
    cache.set(k, data)
    if also_save_to_db:
        File.save_file(data, magic.from_buffer(data, mime=True))
