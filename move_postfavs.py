from database import *
import time

@create_table
class PostFavsOld(MyModel):
    post = ForeignKeyField(Post)
    user = ForeignKeyField(User)
    class Meta:
        table_name = 'postfavs_old'

while 1:
    start = time.time()
    ids = []
    with db.atomic():
        for i in PostFavsOld.select(PostFavsOld.id).limit(5000).iterator():
            ids.append(i.id)
        PostFavs.insert_from(PostFavsOld.select(PostFavsOld.user, PostFavsOld.post).where(PostFavsOld.id.in_(ids)), fields=[PostFavsOld.user, PostFavsOld.post]).execute()
        PostFavsOld.delete().where(PostFavsOld.id.in_(ids)).execute()

    print('moving IDs', ids[0], '..', ids[-1], 'took', time.time()-start, '(', len(ids), 'rows with ', (time.time()-start) / len(ids), 'per row)')
