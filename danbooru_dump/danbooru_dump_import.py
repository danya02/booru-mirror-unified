import sys
sys.path.append('..')
from booru_db import BooruDatabase
from database import *
import datetime
import json
import traceback
import random
import time
import logging
import redis
import collections

r = redis.Redis(db=12)

bdb = BooruDatabase('danbooru')

def parse_date(val):
    try:
        v = datetime.datetime.strptime(val, '%Y-%m-%d %H:%M:%S %Z')
    except:
        val = val.split(' ')
        val[1] = val[1] + '0'
        val = ' '.join(val)
        v = datetime.datetime.strptime(val, '%Y-%m-%d %H:%M:%S.%f %Z')
    if v == datetime.datetime(1970, 1, 1, 0, 0):
        return None
    return v

def insert_row(struct, skip_if_exists=False):
    print(struct['id'])
    post = Post()
    post_existing = bdb.post[ int(struct['id']) ]
    if skip_if_exists and (post_existing is not None):
        return None
    if post_existing:
        post = post_existing

    #print(post, post.__data__, flush=True)
    post.local_id = struct['id']
    #print(post.post_created_at, parse_date(struct['created_at']))
    #print(post.post_updated_at, parse_date(struct['updated_at']))
    #input()
    post.post_created_at = parse_date(struct['created_at'])
    post.post_updated_at = parse_date(struct['updated_at'])
    post.row_updated_at = datetime.datetime.now()
    post.rating = struct['rating']
    post.source = struct['source'] or None
    post.score = struct['score']
    post.parent_local_id = int(struct['parent_id']) or None
    post.uploaded_by = User.get_or_create(board=bdb.booru, local_id=int(struct['uploader_id']))[0]
    bdb.post[ int(struct['id']) ] = post
    post = bdb.post[ int(struct['id']) ]
    #print(post, post.__data__, flush=True)

    old_tags = sorted(bdb.tag[post])
    new_tags = sorted([tag['name'] for tag in struct['tags']])
    if old_tags != new_tags:
        #print('updating tag set', old_tags, new_tags)
        bdb.tag[post] = new_tags
    else:
        pass
        #print('skipped tag set')


    postfavs_existing = set( [i[0] for i in PostFavs.select(User.local_id).join(User).where(PostFavs.post==post).tuples()] )
    postfavs_new = set([int(i) for i in struct['favs']])
    diff_set = postfavs_existing.symmetric_difference(postfavs_new)
    if diff_set:
        #print('fav set difference so updating', postfavs_existing, '->', postfavs_new)

        fav_users = dict()
        for i in User.select().where(User.board == bdb.booru).where(User.local_id.in_([int(i) for i in struct['favs']])):
            fav_users[i.local_id] = i

        PostFavs.delete().where(PostFavs.post == post).execute()

        postfavs_list = []
        for fav in struct['favs']:
            fav = int(fav)
            if fav not in fav_users:
                fav_users[fav] = User.get_or_create(board=bdb.booru, local_id=int(fav))[0]
            postfavs_list.append(PostFavs(post=post, user=fav_users[fav]))

        PostFavs.bulk_create(postfavs_list)


    danpost = DanbooruPostMetadata.get_or_none(post=post) or DanbooruPostMetadata(post=post)
    #print(danpost.__data__)
    danpost.up_score = int(struct['up_score'])
    danpost.down_score = int(struct['up_score'])
    danpost.pixiv_id = int(struct['pixiv_id']) or None
    danpost.approved_by = User.get_or_create(board=bdb.booru, local_id=int(struct['approver_id']))[0]

    danpost.is_pending = struct['is_pending']
    danpost.is_flagged = struct['is_flagged']
    danpost.is_deleted = struct['is_deleted']
    danpost.is_banned = struct['is_banned']
    danpost.is_status_locked = struct['is_status_locked']
    danpost.is_note_locked = struct['is_note_locked']
    try:
        danpost.save(force_insert=True)
    except IntegrityError:
        danpost.save()

    imgpost = ImageMetadata.get_or_none(post=post) or ImageMetadata(post=post)
    #print(imgpost.__data__)
    imgpost.image_width = int(struct['image_width'])
    imgpost.image_height = int(struct['image_height'])
    imgpost.file_size = int(struct['file_size'])
    imgpost.md5 = struct['md5']
    try:
        imgpost.save(force_insert=True)
    except IntegrityError:
        imgpost.save()

def insert_row_atomic(*args, **kwargs):
    with db.atomic():
        return insert_row(*args, **kwargs)

#insert_row_atomic(json.loads(input()))

class CountHandler(logging.Handler):
    def __init__(self):
        self.count = 0
        super().__init__()

    def emit(self, record):
        self.count += 1

    def handle(self, record):
        self.count += 1

    def reset(self):
        self.count = 0

counter = CountHandler()
logger = logging.getLogger('peewee')
logger.addHandler(counter)
logger.setLevel(logging.DEBUG)

class DanbooruDumpRow(MyModel):
    post_id = IntegerField()
    content = TextField()

DANBOORU = Imageboard.get(name='danbooru')
POST_ENTITY, _ = EntityType.get_or_create(name='post')

when = time.time()
#for row in DanbooruDumpRow.select().order_by(fn.Rand()):

def infinite_counter():
    i = 0
    while 1:
        i += 1
        yield i

start = time.time()

try:
    start_times = collections.deque(maxlen=50)
    to_delete = collections.deque()
    for ind in db.batch_commit(infinite_counter(), 100):
    #    count = DanbooruDumpRow.select(fn.COUNT(DanbooruDumpRow.id)).scalar()
    #    if not count:
    #        print('All done!!!')
    #        break
    #    row = DanbooruDumpRow.select().where(DanbooruDumpRow.id >= random.randint(0, count)).get()
        start_times.append(time.time())
        rid = r.randomkey()
        row = r.get(rid)
    #    print('selecting row', rid, 'took', time.time()-when, 'seconds and', counter.count, 'queries')
        when = time.time()
        insert_row_atomic(json.loads(row))
        print('inserting took', time.time()-when, 'seconds and', counter.count, 'queries')
        when = time.time()
        ImportedEntity.get_or_create(entity_type=POST_ENTITY, board=DANBOORU, entity_local_id=rid, final=False)
        to_delete.append(rid)
        while len(to_delete) > 500:
            v = to_delete.popleft()
            print('deleting from redis', v)
            r.delete(v)
    #    print('removing took', time.time()-when, 'seconds and', counter.count, 'queries')
        counter.reset()
        when = time.time()
        print('Current rate:', len(start_times) / (when - start_times[0]))
        print()
except KeyboardInterrupt:
    input('Stopped, enter to continue or another ^C to quit')
