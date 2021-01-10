import sys
sys.path.append('..')
from database import *
import traceback
import os
import shutil
#import threading
import queue
import time
import magic
import redis
import functools
import concurrent.futures

r = redis.Redis()
rc = redis.Redis(db=2)
#rhashes_mimes = redis.Redis(db=2)

BOARD, _ = Imageboard.get_or_create(name='danbooru', base_url='https://danbooru.donmai.us')

started_work_at = time.time()
jobs_executed = 0
find_files = False

executor = concurrent.futures.ThreadPoolExecutor()

#@create_table
class FileToImportTMP(MyModel):
    path = CharField(unique=True)
    locked = BooleanField(default=False)

@functools.lru_cache(maxsize=None)
def get_mimetype(name):
    mimetype_row, _ = MimeType.get_or_create(name=name)
    return mimetype_row

def time_it(fn):
    def exec(*args, **kwargs):
        started = time.time()
        res = fn(*args, **kwargs)
        return time.time()-started, res
    return exec

def data_import(path, thread=None, select_row_time=None):
    started_at = time.time()
    n=6
    ext = path.split('.')[-1]

    read_start = time.time()
    id = int( path.split('/')[-1].split('.')[0] )
    res = rc.get(path)
    if res is not None:
        res = str(res, 'utf-8')
        sha256, mt_str = res.split(' ', 1)
        read_time = time.time() - read_start
        compute_time = 0
    else:
        @time_it
        def read_data(path=path):
            data = rc.get(path)
            if data is None:
                handle = open(path, 'rb')
                data = handle.read()
                handle.close()
            if data == b'':
                print('!!!! file at', path, 'is empty!!')
                return None, None
            return id, data
    
        read_time, a = read_data()
        id, data = a
        if id is None: return

        @time_it
        def get_mimetype_from_file(data=data):
            mimetype = magic.from_buffer(data, mime=True)
            return mimetype

        mimetype_get_time, mt_str = get_mimetype_from_file()
        compute_time = mimetype_get_time
    
        @time_it
        def hashit(data=data):
            return sha256_hash(data)
    
        hash_time, sha256 = hashit()
        compute_time += hash_time
        if os.path.exists(get_path(sha256)):
            raise FileExistsError()
    print(f'id={id}\tread={round(read_time, n)}', end='\t')


    print(f'compute={round(compute_time, n)}', end='\t')

    @time_it
    def db_ops(id=id, mt_str=mt_str, sha256=sha256):
        post = Post.get(Post.board==BOARD, Post.local_id==id)
        mt_row = get_mimetype(mt_str)
        Content.get_or_create(post=post, sha256_current=sha256, mimetype_id=mt_row.id, file_size_current=os.path.getsize(path), ext=ext)
        return None

    db_ops_time, _ = db_ops()


    @time_it
    def save():
        ensure_dir(sha256)
        #print(path, '->', IMAGE_DIR+get_path(sha256))
        shutil.move(path, IMAGE_DIR+get_path(sha256)+'.'+ext)
        return None

    save_file_time, _ = save()

    print(f'db_ops={round(db_ops_time, n)}', f'save={round(save_file_time, n)}', sep='\t', end='\t')

    ended_at = time.time()

    global jobs_executed
    global time_running
    time_on_job = ended_at - started_at
    jobs_executed += 1
    avg_time = (time.time() - started_work_at) / jobs_executed

    print(f'time={round(time_on_job, n)}', f'avg={round(avg_time,n)}', f'jobs={jobs_executed}', sep='\t')


path = '/hugedata/booru/danbooru2020/danbooru2020/original'
if __name__ == '__main__':
    while True:
        started_at = time.time()
        if rc.dbsize() or False:
            path = rc.randomkey()
            path = str(path, 'utf8')
            print('Using cached', path)
        else:
            path, locked = '', 1
            while locked:
                path = r.randomkey()
                l = r.get(path)
                locked = l==b'1'
                path = str(path, 'utf8')
        r.set(path, '1')
        try:
            data_import(path, select_row_time=time.time()-started_at)
            r.delete(path)
            rc.delete(path)
        except:
            traceback.print_exc()
            continue
