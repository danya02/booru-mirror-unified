from peewee import *
import datetime
from PIL import Image
import hashlib
import io
import os

IMAGE_DIR = '/hugedata/booru'

#db = SqliteDatabase(SITE+'.db', timeout=600)
db = MySQLDatabase('unifiedbooru', user='booru', password='booru', host='10.0.0.2')

class MediumBlobField(BlobField):
    field_type = 'MEDIUMBLOB'

content_databases = dict()

def get_content_db(board_name, name):
    database = content_databases.get( (board_name, name[:2]) )
    if database is None:
        try:
            os.makedirs(IMAGE_DIR+'/'+board_name)
        except FileExistsError:
            pass
    database = SqliteDatabase(IMAGE_DIR+'/'+booru_name+'/'+name[:2]+'.db', timeout=300)
    content_databases[ (booru_name, name[:2]) ] = database
    return database

class File(Model):
    name = CharField(unique=True, primary_key=True)
    content = BlobField()
    mimetype = CharField()

    @staticmethod
    def get_file_content(booru, name):
        database = get_content_db(booru, name)
        with database.bind_ctx((File,)):
            database.create_tables((File,))
            file = File.get(File.name==name).content
        return file

    @staticmethod
    def set_file_content(booru, name, data):
        database = get_content_db(booru, name)
        with database.bind_ctx((File,)):
            database.create_tables((File,))
            try:
                filerow = File.get(File.name == name)
                filerow.content = data
            except File.DoesNotExist:
                filerow = File.create(name=name, content=data)

    @staticmethod
    def delete_file(booru, name):
        database = get_content_db(booru, name)
        with database.bind_ctx((File,)):
            database.create_tables((File,))
            return File.delete().where(File.name==name).execute()


    @staticmethod
    def get_length(name):
        database = get_content_db(name)
        with database.bind_ctx((File,)):
            database.create_tables((File,))
            try:
                return File.select(fn.length(File.content)).where(File.name == name).scalar()
            except File.DoesNotExist:
                return None

import logging
logger = logging.getLogger('peewee')
logger.addHandler(logging.StreamHandler())
#logger.setLevel(logging.DEBUG)

class MyModel(Model):
    class Meta:
        database = db

def create_table(cls):
    db.create_tables([cls])
    return cls


@create_table
class Imageboard(MyModel):
    name = CharField(unique=True)
    base_url = CharField(unique=True)

def Board(backref=None):
    return ForeignKeyField(Imageboard, index=True, backref=backref)

@create_table
class User(MyModel):
    board = Board('users')
    local_id = IntegerField(index=True, null=True)
    username = CharField(null=True)
    row_created_at = DateTimeField(index=True, default=datetime.datetime.now)
    row_updated_at = DateTimeField(index=True, default=datetime.datetime.now)
    class Meta:
        indexes = (
                (('board', 'local_id'), True),
        )

@create_table
class Tag(MyModel):
    name = CharField(unique=True)
    created_at = DateTimeField(index=True, default=datetime.datetime.now)

@create_table
class TagPostCount(MyModel):
    tag = ForeignKeyField(Tag)
    board = Board('tag_counts')
    post_count = IntegerField(index=True, default=0)
    changed_at = DateTimeField(index=True, default=datetime.datetime.now)
    class Meta:
        indexes = (
                (('board', 'tag'), True),
        )

@create_table
class Type(MyModel):
    name = CharField(unique=True)
    created_at = DateTimeField(index=True, default=datetime.datetime.now)

@create_table
class TagType(MyModel):
    board = Board('tag_types')
    tag = ForeignKeyField(Tag, backref='types')
    type = ForeignKeyField(Type, backref='tags')
    class Meta:
        primary_key = CompositeKey('board', 'tag', 'type')


@create_table
class Post(MyModel):
    board = Board('posts')
    local_id = IntegerField(index=True)
    row_created_at = DateTimeField(default=datetime.datetime.now)
    row_updated_at = DateTimeField(index=True, default=datetime.datetime.now)
    post_created_at = DateTimeField(index=True)
    post_updated_at = DateTimeField(index=True, null=True)
    uploaded_by = ForeignKeyField(User, null=True)
    source = TextField(null=True)
    rating = CharField(max_length=1)
    score = IntegerField(index=True)
    parent_id = IntegerField(null=True) # should be a foreign key to self, but this constraint may not be possible to satisfy while inserting


    class Meta:
        indexes = (
                (('board', 'local_id'), True),
        )

@create_table
class PostFavs(MyModel):
    post = ForeignKeyField(Post)
    user = ForeignKeyField(User)
    class Meta:
        indexes = (
                (('post', 'user'), True),
        )

@create_table
class ImageMetadata(MyModel):
    post = ForeignKeyField(Post)
    image_width = IntegerField()
    image_height = IntegerField()
    file_size = IntegerField()

@create_table
class DanbooruPostMetadata(MyModel):
    post = ForeignKeyField(Post)
    up_score = IntegerField(null=True)
    down_score = IntegerField(null=True)
    props = BitField()
    is_rating_locked = props.flag()
    is_status_locked = props.flag()
    is_pending = props.flag()
    is_flagged = props.flag()
    is_deleted = props.flag()
    is_banned = props.flag()
    pixiv_id = IntegerField(null=True)
    last_commented_at = DateTimeField(null=True)
    last_noted_at = DateTimeField(null=True)
    approved_by = ForeignKeyField(User)

@create_table
class Content(MyModel):
    post = ForeignKeyField(Post, backref='content')
    path = CharField(unique=True)
    file_size = IntegerField()
    we_modified = BooleanField()

@create_table
class Comment(MyModel):
    post = ForeignKeyField(Post, backref='comments')
    local_id = IntegerField(index=True)
    body = TextField()
    creator = ForeignKeyField(User, backref='comments')
    class Meta:
        indexes = (
                (('post', 'local_id'), True), 
        )

@create_table
class PostTag(MyModel):
    tag = ForeignKeyField(Tag, backref='posts')
    post = ForeignKeyField(Post, backref='tags')
    class Meta:
        primary_key = CompositeKey('tag', 'post')

