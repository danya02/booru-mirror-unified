from database import *
import os
board =Imageboard.get(name='danbooru')
for row in ContentOld.select().join(Post).where(Post.board == board).limit(100000).offset(200000).iterator():
    path = ('/hugedata/booru_old/danbooru2020/danbooru2020/original/0'+str(row.post.local_id)[-3:].zfill(3)+'/'+str(row.post.local_id)+'.'+(row.ext or '*'))
    try:
#        os.unlink(path)
        os.system('rm '+path)
        print(path, 'deleted')
    except FileNotFoundError:
        print(path, 'not found')
