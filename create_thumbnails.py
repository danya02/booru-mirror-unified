from database import *
import subprocess
import os
import datetime

mimetype_jpg = MimeType.get(MimeType.ext == 'jpg')

def create_thumbnail(content, skip_existing=True, save_to_db=False):
    print(content)
    if content.mimetype_id != mimetype_jpg.id:
        return
    if len(content.thumbnail) and skip_existing:
        return
    original_path = IMAGE_DIR + get_path(content.sha256_current) + '.' + content.mimetype.ext
    thumb_path = IMAGE_DIR + get_thumbnail_path(content.sha256_current) + '.' + mimetype_jpg.ext
    old_path = IMAGE_DIR + 'old/' + get_path(content.sha256_current)
    if not os.path.isfile(thumb_path) or False:
        ensure_dir(content.sha256_current)
        cmd = ['convert', original_path, '-thumbnail', '256', thumb_path]
        print(cmd)
        subprocess.run(cmd)
        ct, create = ContentThumbnail.get_or_create(content=content, mimetype=mimetype_jpg)
        if not create:
            ct.generated_at = datetime.datetime.now()
            ct.save()
    else: print('skipping', content)
#    try:
#        open(old_path).close()
    if os.path.exists(old_path):
        print('deleted', old_path)
        os.unlink(old_path)
#    except FileNotFoundError: pass

page = 0
iterated = True
while iterated:
    print('============= PAGE', page, '===============')
    iterated = False
    for c in Content.select().paginate(page).iterator():
        create_thumbnail(c)
        iterated = True
    page += 1
