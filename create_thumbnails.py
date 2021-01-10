from database import *
import subprocess
import os

def create_thumbnail(content, skip_existing=True, save_to_db=False):
    original_path = IMAGE_DIR + get_path(content.sha256_current) + '.' + content.ext
    thumb_path = IMAGE_DIR + get_thumbnail_path(content.sha256_current) + '.' + content.ext
    if not os.path.isfile(thumb_path) or not skip_existing:
        ensure_dir(content.sha256_current)
        cmd = ['convert', original_path, '-thumbnail', '256', thumb_path]
        print(cmd)
        subprocess.run(cmd)

    if save_to_db:
        content.has_thumbnail = True
        content.save(only=[Content.has_thumbnail])

while Content.select(fn.COUNT(1)).where(Content.has_thumbnail==False).scalar():
    for c in Content.select().where(Content.has_thumbnail==False).limit(5000).iterator():
        create_thumbnail(c)
