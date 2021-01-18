import database
from peewee import *
import datetime
import functools
import magic
import hashlib
import os

BOARD, _ = database.Imageboard.get_or_create(name='rule34', base_url='https://rule34.xxx')

olddb = MySQLDatabase('rule34_bk', user='booru', password='booru')

class OldModel(Model):
    deleted = BooleanField(index=True, default=False)  # this is used so as not to remove rows from the database but still keep track of those which have been processed. This column must be manually added to all tables.

    class Meta:
        database = olddb

def confirm_delete(row):
    print('OK to delete', repr(row), str(row), row.__dict__)
    return True or input('y/n> ').lower() == 'y'

IMAGE_DIR = '/hugedata/booru_old/rule34.xxx/'

#db = SqliteDatabase(SITE+'.db', timeout=600)
db = MySQLDatabase('rule34', user='booru', password='booru', host='10.0.0.2')

content_databases = dict()

def get_content_db(name):
    database = content_databases.get(name[:2])
    if database is None:
        try:
            os.makedirs(IMAGE_DIR+name[:1]+'/')
        except FileExistsError:
            pass
        database = SqliteDatabase(IMAGE_DIR+name[:1]+'/'+name[:2]+'.db', timeout=300)
        content_databases[name[:2]] = database
    return database

class File(Model):
    name = CharField(unique=True, primary_key=True)
    content = BlobField()

    @staticmethod
    def get_file_content(name):
        database = get_content_db(name)
        with database.bind_ctx((File,)):
            database.create_tables((File,))
            return File.get(File.name==name).content

    @staticmethod
    def delete_file(name):
        database = get_content_db(name)
        with database.bind_ctx((File,)):
            database.create_tables((File,))
            #return File.delete().where(File.name==name).execute()

class AccessLevel(OldModel):
    name = CharField(unique=True)

    def migrate(self):
        row, _ = database.AccessLevel.get_or_create(name=self.name)
        return row

class User(OldModel):
    id = IntegerField(primary_key=True, unique=True)
    username = CharField(null=True)
    level = ForeignKeyField(AccessLevel, null=True)
    join_date = DateField(null=True)

    def migrate(self):
        row, _ = database.User.get_or_create(board=BOARD, local_id=self.id, username=self.username)

        if self.join_date:
            database.UserJoinDate.get_or_create(user=row, join_date=self.join_date)             

        if self.level:
            database.UserAccessLevel.get_or_create(user=row, level=self.level.migrate())

        #count = Post.select(fn.COUNT(1)).where(Post.creator == self).scalar() + Comment.select(fn.COUNT(1)).where(Comment.author == self).scalar() + Note.select(fn.COUNT(1)).where(Note.author == self).scalar()
        #if count == 0:
        #    if confirm_delete(self):
        #        self.delete_instance()

        return row

class Rating(OldModel):
    value = CharField(unique=True)

class Status(OldModel):
    value = CharField(unique=True)

    def migrate(self):
        row, _ = database.Status.get_or_create(value=self.value)
        return row

class Tag(OldModel):
    name = CharField(unique=True)

    def migrate(self):
        row, _ = database.Tag.get_or_create(name=self.name)
        return row


class Type(OldModel):
    name = CharField(unique=True)

    def migrate(self):
        row, _ = database.Type.get_or_create(name=self.name)
        return row

class TagPostCount(OldModel):
    tag = ForeignKeyField(Tag, primary_key=True)
    value = IntegerField()

class TagType(OldModel):
    tag = ForeignKeyField(Tag)
    type = ForeignKeyField(Type)
    
    def migrate(self):
        type = self.type.migrate()
        tag = self.tag.migrate()
        database.TagType.get_or_create(board=BOARD, type=type, tag=tag)
        if confirm_delete(self):
            TagType.update(TagType.deleted == True).where(TagType.tag == self.tag).where(TagType.type == self.type).execute()

    class Meta:
        primary_key = CompositeKey('tag', 'type')

class UnavailablePost(OldModel):
    id = IntegerField(primary_key=True, unique=True)
    reason = TextField(null=True)
    first_detected_at = DateTimeField(default=datetime.datetime.now)

class Post(OldModel):
    id = IntegerField(primary_key=True, unique=True)

    width = IntegerField()
    height = IntegerField()
    url = CharField(unique=True)

    sample_width = IntegerField()
    sample_height = IntegerField()
    sample = CharField()

    preview_width = IntegerField()
    preview_height = IntegerField()
    preview = CharField()

    md5 = CharField(unique=True)
    created_at = DateTimeField()
    changed_at = DateTimeField()
    score = IntegerField()
    creator = ForeignKeyField(User)
    rating = ForeignKeyField(Rating)
    status = ForeignKeyField(Status)
    source = CharField()
    
    parent = ForeignKeyField('self', backref='children', null=True)

    def migrate(self):
        post, _ = database.Post.get_or_create(board=BOARD,
                local_id=self.id,
                post_created_at=self.created_at,
                post_updated_at=self.changed_at,
                uploaded_by=self.creator.migrate(),
                source=(self.source or None),
                rating=self.rating.value,
                score=self.score, parent_id=None if self.parent is None else self.parent.id)

        database.ImageMetadata.get_or_create(post=post, image_width=self.width, image_height=self.height, md5=self.md5)
        database.PreviewSizeInfo.get_or_create(post=post, sample_width=self.sample_width, sample_height=self.sample_height, preview_width=self.preview_width, preview_height=self.preview_height)
        database.ImageURL.get_or_create(post=post, img_url=self.url, sample_url=self.sample, preview_url=self.preview)
        database.PostStatus.get_or_create(post=post, status=database.Status.get_or_create(value=self.status.value)[0])
        for i in self.comments:
            i.migrate(post)
        for i in self.notes:
            i.migrate(post)

        tags = []
        for i in self.tags:
            tags.append(i.tag.migrate())

        data = []
        for i in tags:
            data.append({'post': post, 'tag': i})

        try:
            database.PostTag.insert_many(data).execute()
        except:
            with db.atomic():
                for i in self.tags:
                    i.migrate(post_row=post)
        PostTag.update(deleted=True).where(PostTag.post==self).where(PostTag.tag.in_(tags)).execute()
        if len(self.content):
            self.content.get().migrate(post)
        self.deleted = True
        self.save()
        return post


class Content(OldModel):
    post = ForeignKeyField(Post, backref='content')
    path = CharField(unique=True)
    original_length = IntegerField()
    current_length = IntegerField()

    def migrate(self, post_row=None):
        data = File.get_file_content(self.path)
        sha256 = hashlib.sha256(data).hexdigest()
        mt = magic.from_buffer(data, mime=True)
        mt_row, _ = database.MimeType.get_or_create(name=mt)
        ext = self.path.split('.')[-1]
        row, _ = database.Content.get_or_create(post=post_row or self.post.migrate(),
                sha256_current=sha256,
                mimetype=mt_row,
                file_size_current=len(data),
                we_modified_it=False)
        database.File.save_file(data, ext, raise_if_exists=True, but_check_for_same=True)
        self.deleted = True
        self.save()
        return row

class PostTag(OldModel):
    tag = ForeignKeyField(Tag, backref='posts')
    post = ForeignKeyField(Post, backref='tags')
    class Meta:
        primary_key = CompositeKey('tag', 'post')

    def migrate(self, post_row=None):
        database.PostTag.get_or_create(post=post_row or self.post.migrate(), tag=self.tag.migrate())
#        if confirm_delete(self):
#            PostTag.update(deleted=True).where(PostTag.tag==self.tag).where(PostTag.post==self.post).execute()

class Comment(OldModel):
    author = ForeignKeyField(User, backref='comments')
    post = ForeignKeyField(Post, backref='comments')
    id = IntegerField(primary_key=True, unique=True)
    body = TextField()
    score = IntegerField()
    created_at = DateTimeField()

    def migrate(self, post_row=None):
        row, _ = database.Comment.get_or_create(post=post_row or self.post.migrate(),
                local_id=self.id,
                creator=self.author.migrate(),
                body=self.body,
                score=self.score,
                comment_created_at=self.created_at)
        self.deleted = True
        self.save()
        return row

class Note(OldModel):
    id = IntegerField(primary_key=True, unique=True)
    author = ForeignKeyField(User, backref='notes')
    post = ForeignKeyField(Post, backref='notes')
    body = TextField()
    created_at = DateTimeField()
    updated_at = DateTimeField()
    is_active = BooleanField()
    version = IntegerField()

    x = IntegerField()
    y = IntegerField()
    width = IntegerField()
    height = IntegerField()

    def migrate(self, post_row=None):
        row, _ = database.Note.get_or_create(
                board = BOARD,
                local_id = self.id,
                author = self.author.migrate(),
                post = post_row or self.post.migrate(),
                body = self.body,
                note_created_at = self.created_at,
                note_updated_at = self.updated_at,
                is_active = self.is_active,
                version = self.version,
                x = self.x, y = self.y, width = self.width, height = self.height)
        self.deleted = True
        self.save()
        return row

page = 1
iterated = True
while iterated:
    iterated = False
    #for i in Post.select().where(Post.deleted == False).where(Post.status_id.in_([3, 16])).limit(1).iterator():
    for i in Content.select().where(Content.deleted == False).limit(1).iterator():
        i.migrate()
        iterated = True
    page += 1

