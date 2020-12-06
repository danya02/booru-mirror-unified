from peewee import *
import datetime
from PIL import Image
import hashlib
import io
import os
import random
import hashlib

IMAGE_DIR = '/hugedata/booru/unified'

#db = SqliteDatabase('test.db', timeout=600)
db = MySQLDatabase('unifiedbooru', user='booru', password='booru', host='10.0.0.2')

class MediumBlobField(BlobField):
    field_type = 'MEDIUMBLOB'

content_databases = dict()

def sha256_hash(data):
    hasher = hashlib.sha256()
    hasher.update(data)
    return hasher.hexdigest()

def get_content_db(name):
    database = content_databases.get( name[:2] )
    if database is None:
        try:
            os.makedirs(IMAGE_DIR)
        except FileExistsError:
            pass
        database = SqliteDatabase(IMAGE_DIR+'/'+name[:2]+'.db', timeout=300)
    content_databases[ name[:2] ] = database
    return database

class File(Model):
    sha256 = CharField(unique=True, primary_key=True)
    mimetype = CharField()
    content = BlobField()

    @staticmethod
    def get_file_by_sha256(name):
        database = get_content_db(name)
        with database.bind_ctx((File,)):
            database.create_tables((File,))
            file = File.get(File.name==name).content
        return file

    @staticmethod
    def save_file(data):
        name = sha256_hash(data)
        database = get_content_db(booru, name)
        with database.bind_ctx((File,)):
            database.create_tables((File,))
            try:
                filerow = File.get(File.name == name)
                filerow.content = data
            except File.DoesNotExist:
                filerow = File.create(name=name, content=data)

    @staticmethod
    def delete_file_by_sha256(name):
        database = get_content_db(name)
        with database.bind_ctx((File,)):
            database.create_tables((File,))
            return File.delete().where(File.sha256==name).execute()


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
#logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

class MyModel(Model):
    class Meta:
        database = db

def create_table(cls):
    db.create_tables([cls])
    return cls

class TinyIntegerField(SmallIntegerField):
    field_type = 'tinyint'

@create_table
class Imageboard(MyModel):
    id = TinyIntegerField(primary_key=True)
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
class UserJoinDate(MyModel):
    user = ForeignKeyField(User, primary_key=True)
    join_date = DateTimeField()

@create_table
class AccessLevel(MyModel):
    id = SmallIntegerField(primary_key=True)
    name = CharField(unique=True)

@create_table
class UserAccessLevel(MyModel):
    user = ForeignKeyField(User, primary_key=True)
    level = ForeignKeyField(AccessLevel)

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
    id = SmallIntegerField(primary_key=True)
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
    user = ForeignKeyField(User)
    post = ForeignKeyField(Post)
    class Meta:
        indexes = (
                (('post', 'user'), True),
        )
        primary_key = CompositeKey('user', 'post')

@create_table
class ImageMetadata(MyModel):
    post = ForeignKeyField(Post, primary_key=True)
    image_width = IntegerField(index=True)
    image_height = IntegerField(index=True)
    file_size = IntegerField(index=True, null=True)

@create_table
class PreviewSizeInfo(MyModel):
    post = ForeignKeyField(Post, primary_key=True)
    sample_width = IntegerField()
    sample_height = IntegerField()

    preview_width = IntegerField()
    preview_height = IntegerField()

@create_table
class ImageURL(MyModel):
    post = ForeignKeyField(Post, primary_key=True)
    img_url = CharField()
    sample_url = CharField()
    preview_url = CharField()

@create_table
class Status(MyModel):
    id = TinyIntegerField(primary_key=True)

@create_table
class DanbooruPostMetadata(MyModel):
    post = ForeignKeyField(Post, primary_key=True)
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
class MimeType(MyModel):
    id = SmallIntegerField(primary_key=True)
    name = CharField(unique=True)

@create_table
class Content(MyModel):
    post = ForeignKeyField(Post, backref='content')
    path = CharField(index=True)
    mimetype = ForeignKeyField(MimeType, backref='contents')
    file_size_when_acquired = IntegerField(index=True)
    file_size_current = IntegerField(index=True)
    we_modified_it = BooleanField(index=True)

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

@create_table
class EntityType(MyModel):
    name = CharField(unique=True)

@create_table
class QueuedImportEntity(MyModel):
    board = ForeignKeyField(Imageboard)
    entity_type = ForeignKeyField(EntityType)
    entity_local_id = IntegerField()
    additional_data = TextField(null=True)
    enqueued_at = DateTimeField(default=datetime.datetime.now, index=True)
    errors_encountered = IntegerField(null=True, index=True)
    priority = FloatField(default=random.random, index=True)
    row_locked = BooleanField(index=True)

    @staticmethod
    def tasks_query(board=None, type=None, with_less_than_errors=10):
        query = QueuedImportEntity.select()
        if board:
            if isinstance(board, 'str'):
                board = Imageboard.get(Imageboard.name==board)
            query = query.where(QueuedImportEntity.board == board)
        
        if type:
            if isinstance(type, str):
                type = EntityType.get(EntityType.name == type)
            query = query.where(QueuedImportEntity.type == type)
        query = query.where(QueuedImportEntity.row_locked == False).where(QueuedImportEntity.errors_encountered <= with_less_than_errors)
        query = query.order_by(-QueuedImportEntity.priority)
        return query

    def report_error(self, error):
        with db.atomic():
            ImportEntityError.create(queued_entity=self, error=error)
            self.errors_encountered = (self.errors_encountered or 0) + 1
            self.priority = random.random()
            self.row_locked = False
            self.save()

    def report_success(self, and_delete_self=True, and_mark_as_final=False):
        with db.atomic():
            imported, did_create = ImportedEntity.get_or_create(board=self.board, entity_type=self.entity_type, entity_local_id=self.entity_local_id, additional_data=self.additional_data)
            if not did_create:
                imported.latest_update_at = datetime.datetime.now()
                imported.save()
            if and_delete_self:
                self.delete_instance()


    class Meta:
        indexes = (
                (('board', 'entity_type', 'entity_local_id'), True), 
                )

@create_table
class ImportEntityError(MyModel):
    queued_entity = ForeignKeyField(QueuedImportEntity, index=True, on_delete='CASCADE')
    when = DateTimeField(default=datetime.datetime.now)
    error = TextField()

@create_table
class ImportedEntity(MyModel):
    board = ForeignKeyField(Imageboard)
    entity_type = ForeignKeyField(EntityType)
    entity_local_id = IntegerField(index=True)
    additional_data = TextField(null=True)
    latest_update_at = DateTimeField(default=datetime.datetime.now, index=True)
    final = BooleanField()
    
    def enqueue_again(self, ignore_final=False):
        if (self.final and ignore_final) or (not self.final):
            QueuedImportEntity.create(board=self.board, entity_type=self.entity_type, entity_local_id=self.entity_local_id, additional_data=self.additional_data)
        else:
            raise ValueError('not allowed to enqueue a final entity')


    class Meta:
        indexes = (
                (('board', 'entity_type', 'entity_local_id'), True), 
                )


