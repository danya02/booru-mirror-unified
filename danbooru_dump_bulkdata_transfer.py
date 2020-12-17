from database import *
import traceback
import os
#import threading
import queue
import time
import magic
import buffer
import lockedfiles

BOARD, _ = Imageboard.get_or_create(name='danbooru', base_url='https://danbooru.me')

started_work_at = time.time()
jobs_executed = 0
time_running = 0

def data_import(path, thread=None):
    started_at = time.time()
    id = int( path.split('/')[-1].split('.')[0] )
    if not lockedfiles.take(id):
        return
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
    lockedfiles.release(id)
    unlinked_at = time.time()

    n = 6
    read_time = round( read_at - started_at, n)
    wrote_time = round( saved_at - read_at, n)
    db_ops_time = round( db_operations_done_at - saved_at, n)
    unlink_time = round( unlinked_at - db_operations_done_at, n)

    global jobs_executed
    global time_running
    time_running += (read_time + wrote_time + db_ops_time + unlink_time)
    jobs_executed += 1
    #running_time = unlinked_at - started_work_at
    time_per_job = time_running / jobs_executed

    print(str(thread)+'#' if thread is not None else '', 'img_id:', str(id).ljust(7), '\tread:', str(read_time).ljust(n+2, '0'), '\twrote:', str(wrote_time).ljust(n+2, '0'), '\tdb_ops:', str(db_ops_time).ljust(n+2, '0'), '\tunlink:', str(unlink_time).ljust(n+2, '0'), '\tsaved at id:', content_row, '\t', 'avg_time_per_job:', round(time_per_job, n), 'jobs:', jobs_executed)

path = '/hugedata/booru/danbooru2019/danbooru2019/original'
handle = os.popen('find "'+path+'" -type f')

for path in handle:
    path = path.strip()
    try:
        data_import(path)
    except:
        traceback.print_exc()
