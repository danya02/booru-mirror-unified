from database import *
import os
import hashlib
import redis
import traceback

r = redis.Redis(db=2)

mime = 'image/png'
ext = 'png'

for row in ContentOld.select().where(ContentOld.mimetype == MimeType.get(name=mime)).limit(20000).offset(0):
    if Post.select(fn.Count(1)).where(Post.id == row.post_id).scalar() == 0:
        print('post does not exist', row.post_id)
        continue
    try:
        old_path = IMAGE_DIR + 'old/' + get_path(row.sha256_current)
        new_path = IMAGE_DIR + get_path(row.sha256_current) + '.' + ext
        if not os.path.exists(old_path):
            if os.path.exists(new_path):
                print('old not found, but new found. checking hash')
                with open(new_path, 'rb') as handle:
                    data = handle.read()
                sha256 = hashlib.sha256(data).hexdigest()
                if sha256 != row.sha256_current:
                    raise Exception('existing row', str(row), repr(row), 'with wrong sha256: of file ', new_path, 'hash', sha256)
                else:
                    print('hash check okay')
        else:
            ensure_dir(row.sha256_current)
            os.rename(old_path, new_path)
            print(old_path, '->', new_path)
        new_row = Content(post_id=row.post_id, sha256_current=row.sha256_current, mimetype_id=row.mimetype_id, file_size_current=row.file_size_current, we_modified_it=row.we_modified_it)
        try:
            new_row.save(force_insert=True)
        except IntegrityError:
            print('row with hash', row.sha256_current, 'exists already, updating')
            new_row = Content.get(Content.sha256_current == row.sha256_current)
            new_row.post_id = row.post_id
            new_row.mimetype_id = row.mimetype_id
            new_row.file_size_current = row.file_size_current
            new_row.we_modified_it = row.we_modified_it
            new_row.save()
    #    row.delete_instance()
        r.set(row.id, '0')
        print('enqueued delete', row.id)
    except:
        traceback.print_exc()
        continue
