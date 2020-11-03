from booru_db import BooruDatabase
from database import *
import datetime
import json
import traceback

bdb = BooruDatabase('danbooru')

os.chdir('/hugedata/booru/danbooru2019/danbooru2019/metadata')

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

def insert_row(struct, skip_if_exists=True):
    post = Post()
    post_existing = Post.get_or_none(Post.board == bdb.booru, Post.local_id == int(struct['id']))
    if skip_if_exists and (post_existing is not None):
        return None

    post.post_created_at = parse_date(struct['created_at'])
    post.post_updated_at = parse_date(struct['updated_at'])
    post.rating = struct['rating']
    post.source = struct['source'] or None
    post.score = struct['score']
    post.parent_id = int(struct['parent_id']) or None
    post.uploaded_by = User.get_or_create(board=bdb.booru, local_id=int(struct['uploader_id']))[0]
    bdb.post[ int(struct['id']) ] = post

    bdb.tag[post] = [tag['name'] for tag in struct['tags']]

    for fav in struct['favs']:
        fav_user = User.get_or_create(board=bdb.booru, local_id=int(fav))[0]
        PostFavs.get_or_create(post=post, user=fav_user)

    danpost = DanbooruPostMetadata(post=post)
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

    imgpost = ImageMetadata(post=post)
    imgpost.image_width = int(struct['image_width'])
    imgpost.image_height = int(struct['image_height'])
    imgpost.file_size = int(struct['file_size'])
    try:
        imgpost.save(force_insert=True)
    except IntegrityError:
        imgpost.save()

def insert_row_atomic(*args, **kwargs):
    with db.atomic():
        return insert_row(*args, **kwargs)

#insert_row_atomic(json.loads(input()))

files = []
for i in os.popen('find -type f'):
    files.append(i.strip())

files.sort()

current = {'file':None, 'offset': None, 'line': None}

try:
    for file in files:
        with open(file) as handle:
            current['offset'] = 0
            current['line'] = 1
            for line in handle:
                insert_row_atomic(json.loads(line))
                current['file'] = file
                current['offset'] += len(line)
                current['line'] += 1
                print(current)
except:
    traceback.print_exc()
    print('ptr', current)

