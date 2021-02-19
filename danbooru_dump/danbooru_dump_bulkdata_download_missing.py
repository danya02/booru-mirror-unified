import subprocess
import os
import sys
sys.path.append('..')
from database import *
import time
import random
import danbooru_dump_transfer_queue_into_redis_from_find
os.chdir('/hugedata/booru/danbooru_temp')


def download_content(id):
    print         (['rsync', '--recursive', '--verbose', f'rsync://78.46.86.149:873/danbooru2020/original/0{str(id)[-3:].zfill(3)}/{id}.*', './danbooru2020/original/'])
    subprocess.run(['rsync', '--recursive', '--verbose', f'rsync://78.46.86.149:873/danbooru2020/original/0{str(id)[-3:].zfill(3)}/{id}.*', './danbooru2020/original/'])

while 1:
    #time.sleep(3)
    query = Post.select().where(~fn.EXISTS(Content.select().where(Content.post_id == Post.id))).where(Post.board == Imageboard.get(Imageboard.name == 'danbooru')).order_by(random.choice([Post.id, Post.local_id])).limit(200)
    for post in query:
        print(post.__data__)
        download_content(post.local_id)
        danbooru_dump_transfer_queue_into_redis_from_find.find_transfer()

