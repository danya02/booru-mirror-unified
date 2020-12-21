from database import *
import traceback
import os
#import threading
import queue
import time
import magic
import redis

r = redis.Redis()

BOARD, _ = Imageboard.get_or_create(name='danbooru', base_url='https://danbooru.me')

started_work_at = time.time()
jobs_executed = 0
time_running = 0
find_files = False

#@create_table
class FileToImportTMP(MyModel):
    path = CharField(unique=True)
    locked = BooleanField(default=False)

def data_import(path, thread=None, select_row_time=None):
    started_at = time.time()
    id = int( path.split('/')[-1].split('.')[0] )
    mimetype = magic.from_file(path, mime=True)
    mimetype_row, _ = MimeType.get_or_create(name=mimetype)
    handle = open(path, 'rb')
    data = handle.read()
    handle.close()
    datalen = len(data)
    if data == b'':
        print('!!!! file at', path, 'is empty!!')
        return
    read_at = time.time()

    sha256 = File.save_file(data, mimetype)
    saved_at = time.time()

    post = Post.get(Post.board==BOARD, Post.local_id==id)
    content_row, created = Content.get_or_create(post=post, sha256_current=sha256, mimetype=mimetype_row, file_size_current=datalen, we_modified_it=False)
    db_operations_done_at = time.time()

    os.unlink(path)
    unlinked_at = time.time()

    n = 6
    read_time = round( read_at - started_at, n)
    wrote_time = round( saved_at - read_at, n)
    db_ops_time = round( db_operations_done_at - saved_at, n)
    unlink_time = round( unlinked_at - db_operations_done_at, n)

    global jobs_executed
    global time_running
    time_running += (read_time + wrote_time + db_ops_time + unlink_time) + select_row_time
    jobs_executed += 1
    #running_time = unlinked_at - started_work_at
    time_per_job = time_running / jobs_executed
    
    time_on_job = round(read_time+wrote_time+db_ops_time+unlink_time+select_row_time, n)

    print(str(thread)+'#' if thread is not None else '', '' if select_row_time is None else f'select: {round(select_row_time, 6)}\t',  'img_id:', str(id).ljust(7), '\tread:', str(read_time).ljust(n+2, '0'), '\twrote:', str(wrote_time).ljust(n+2, '0'), '\tdb_ops:', str(db_ops_time).ljust(n+2, '0'), '\ttotal:', str(time_on_job).ljust(n+2,'0'), '\tsaved at id:', content_row, '\t', 'avg_time_per_job:', round(time_per_job, n), 'jobs:', jobs_executed)

path = '/hugedata/booru/danbooru2019/danbooru2019/original'
if __name__ == '__main__':
    if find_files:
        handle = os.popen('find "'+path+'" -type f')
        n = 0
        with db.atomic() as txn:
            for path in handle:
                n += 1
                path = path.strip()
                started = time.time()
                print(FileToImportTMP.get_or_create(path=path), time.time()-started)
                if n%100 == 0:
                    txn.commit()

    else:
        while True:
            started_at = time.time()
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
            except:
                traceback.print_exc()
                continue
