from database import *
import subprocess
import os
import datetime

mimetype_jpg = MimeType.get(MimeType.ext == 'jpg')
mimetype_png = MimeType.get(MimeType.ext == 'png')
mts = [mimetype_jpg, mimetype_png]
mtids = [mt.id for mt in mts]

def create_thumbnail(content, skip_existing=True):
    print(content)
    if content.mimetype_id not in mtids:
        return
    if len(content.thumbnail) and skip_existing:
        return
    original_path = IMAGE_DIR + get_path(content.sha256_current) + '.' + content.mimetype.ext
    thumb_path = IMAGE_DIR + get_thumbnail_path(content.sha256_current) + '.' + content.mimetype.ext
    old_path = IMAGE_DIR + 'old/' + get_path(content.sha256_current)
    old_ext_path = IMAGE_DIR + 'old_with_exts/' + get_path(content.sha256_current) + '.' + content.mimetype.ext
    if not os.path.isfile(thumb_path) or not skip_existing:
        ensure_dir(content.sha256_current)
        cmd = ['convert', original_path, '-thumbnail', '256x256', thumb_path]
        print(cmd)
        subprocess.run(cmd)
        size = os.path.getsize(thumb_path)
        ct = ContentThumbnail.get_or_none(content=content, mimetype=content.mimetype)
        if ct is None:
            ct = ContentThumbnail.create(content=content, mimetype=content.mimetype, size=size)
        else:
            ct.size = size
            ct.generated_at = datetime.datetime.now()
            ct.save()
    else: print('skipping', content)
#    try:
#        open(old_path).close()
    if os.path.exists(old_path):
        print('deleted', old_path)
        os.unlink(old_path)
    if os.path.exists(old_ext_path):
        print('deleted', old_ext_path)
        os.unlink(old_ext_path)
#    except FileNotFoundError: pass

page = 0
iterated = True
while iterated:
    print('============= PAGE', page, '===============')
    iterated = False
    for c in Content.select().where(~fn.EXISTS(ContentThumbnail.select().where(ContentThumbnail.content_id==Content.post_id))).join(Post).order_by(Post.local_id).iterator():
        create_thumbnail(c, skip_existing=False)
        iterated = True
    page += 1
