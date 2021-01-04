from database import *
import os
import shutil
import time
import humanize
import datetime

import redis
import tqdm
import traceback

queue = redis.Redis(db=5)

os.chdir('/hugedata/booru_old/unified/files') # copy from
cwd = os.getcwd()

TARGET = '/hugedata/booru/' # copy to

if input('input Q to enqueue paths, anything else to move them').lower() == 'q':
    search = os.popen('find -type f')

    for path in search:
        path = path.strip()
        path = cwd + path[1:]
        print(path)
        queue.set(path, 0)

else:
    dbsize = queue.dbsize()
    dbsize_first = dbsize
    time_started = time.time()
    while dbsize:
        current_time = time.time()
        time_running = current_time - time_started
        changed_dbsize = dbsize - dbsize_first
        speed = changed_dbsize  / time_running
        if speed > 0:
            eta = 'inf'
            human_eta = 'inf'
        else:
            eta = dbsize / (speed or 0.1)
            eta = -eta
            eta_td = datetime.timedelta(seconds=eta)
            human_eta = humanize.precisedelta(eta_td, minimum_unit='seconds')
            eta = round(eta)


        print('cursize:', dbsize, 'change:', changed_dbsize, 'speed:', speed, 'it/s', 'eta:', eta, 'seconds or', human_eta)

        path = queue.randomkey()
        path = str(path, 'utf8')

        filename = path.split('/')[-1]
        ensure_dir(filename)
        target_path = TARGET + get_path(filename)
        try:
            shutil.move(path, target_path)
            queue.delete(path)
        except:
            traceback.print_exc()
        dbsize = queue.dbsize()
