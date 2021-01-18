from peewee import *
import peewee
import datetime
from PIL import Image
import hashlib
import io
import os
import random
import hashlib

IMAGE_DIR = '/hugedata/booru/'
DIR_TREE_DEPTH = 2
DIR_TREE_SEGMENT_LEN = 2
#db = SqliteDatabase('test.db', timeout=600)
db = MySQLDatabase('unifiedbooru', user='booru', password='booru', host='10.0.0.128')

class MediumBlobField(BlobField):
    field_type = 'MEDIUMBLOB'

content_databases = dict()

def sha256_hash(data):
    hasher = hashlib.sha256()
    hasher.update(data)
    return hasher.hexdigest()

def get_dir(name):
    path = ''
    cursor = DIR_TREE_SEGMENT_LEN
    for ind in range(DIR_TREE_DEPTH):
        path += name[:cursor] + '/'
        cursor += DIR_TREE_SEGMENT_LEN
    return path

def get_path(name):
    return get_dir(name) + name

def get_thumbnail_path(name):
    return 'thumbnails/' + get_dir(name) + name

def ensure_dir(name):
    target_dir = get_dir(name)
    os.makedirs(IMAGE_DIR + target_dir, exist_ok=True)
    os.makedirs(IMAGE_DIR + 'thumbnails/' + target_dir, exist_ok=True)

class File:
    @staticmethod
    def save_file(data, ext, raise_if_exists=True, but_check_for_same=False):
        name = sha256_hash(data)
        ensure_dir(name)
        path_no_ext = IMAGE_DIR + get_path(name)
        path = path_no_ext + '.' + ext

        return_value = None
        if (os.path.isfile(path) or os.path.isfile(path_no_ext)) and raise_if_exists:
            if not but_check_for_same:
                raise FileExistsError('file with name', path, '(maybe omitting extension) already exists')
            else:
                if os.path.isfile(path):
                    with open(path, 'rb') as handle:
                        exist_data = handle.read()
                    if data == exist_data:
                        return_value = name
                    else:
                        raise FileExistsError('file with name', path, 'already exists and is different from new data')
                if os.path.isfile(path_no_ext):
                    with open(path_no_ext, 'rb') as handle:
                        exist_data = handle.read()
                    if data == exist_data:
                        print(path_no_ext, 'renamed to', path)
                        os.rename(path_no_ext, path)
                        return_value = name
                    else:
                        raise FileExistsError('file with name', path_no_ext, 'already exists and is different from new data')


        with open(path, 'wb') as handle:
            handle.write(data)
        return name

import logging
logger = logging.getLogger('peewee')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

class ForeignKeyField(peewee.ForeignKeyField):
    @property
    def field_type(self):
        return self.rel_field.field_type.replace('BIGAUTO', 'BIGINT').replace('AUTO_INCREMENT', '').replace('AUTO', 'INTEGER')

class MyModel(Model):
    class Meta:
        database = db
        legacy_table_names = False

def create_table(cls):
    db.create_tables([cls])
    return cls

class TinyIntegerField(SmallIntegerField):
    field_type = 'TINYINT UNSIGNED'

class TinyIntegerAutoField(BigAutoField):
    field_type = 'TINYINT UNSIGNED AUTO_INCREMENT'
    auto_increment = True

class UnsignedSmallIntegerField(IntegerField):
    field_type = 'SMALLINT UNSIGNED'

class SmallIntegerAutoField(BigAutoField):
    field_type = 'SMALLINT UNSIGNED AUTO_INCREMENT'
    auto_increment = True

class UnsignedIntegerField(IntegerField):
    field_type = 'INTEGER UNSIGNED'

class UnsignedAutoField(AutoField):
    field_type = 'INTEGER UNSIGNED AUTO_INCREMENT'
    auto_increment = True

@create_table
class Imageboard(MyModel):
    id = TinyIntegerAutoField(primary_key=True)
    name = CharField(unique=True)
    base_url = CharField(unique=True)

def Board(backref=None, null=None):
    return ForeignKeyField(Imageboard, index=True, backref=backref, null=null)

@create_table
class User(MyModel):
    id = UnsignedAutoField(primary_key=True)
    board = Board('users')
    local_id = UnsignedIntegerField(index=True, null=True)
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
    id = SmallIntegerAutoField(primary_key=True)
    name = CharField(unique=True)


@create_table
class UserAccessLevel(MyModel):
    user = ForeignKeyField(User, primary_key=True)
    level = ForeignKeyField(AccessLevel)

@create_table
class Tag(MyModel):
    id = UnsignedAutoField(primary_key=True)
    name = CharField(unique=True)
    created_at = DateTimeField(index=True, default=datetime.datetime.now)

@create_table
class TagPostCount(MyModel):
    tag = ForeignKeyField(Tag, index=True)
    board = Board('tag_counts', null=True)
    post_count = UnsignedIntegerField(index=True, default=0)
    changed_at = DateTimeField(index=True, default=datetime.datetime.now)
    class Meta:
        indexes = (
                (('board', 'tag'), True),
        )

@create_table
class Type(MyModel):
    id = TinyIntegerAutoField(primary_key=True)
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
    id = UnsignedAutoField(primary_key=True)
    board = Board('posts')
    local_id = UnsignedIntegerField(index=True)
    row_created_at = DateTimeField(default=datetime.datetime.now)
    row_updated_at = DateTimeField(index=True, default=datetime.datetime.now)
    post_created_at = DateTimeField(index=True)
    post_updated_at = DateTimeField(index=True, null=True)
    uploaded_by = ForeignKeyField(User, null=True)
    source = TextField(null=True)
    rating = CharField(max_length=1)
    score = IntegerField(index=True)
    parent_local_id = UnsignedIntegerField(null=True)
    parent = ForeignKeyField('self', null=True)


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
    image_width = UnsignedIntegerField(index=True)
    image_height = UnsignedIntegerField(index=True)
    file_size = IntegerField(index=True, null=True)
    md5 = CharField(index=True, max_length=32)

@create_table
class PreviewSizeInfo(MyModel):
    post = ForeignKeyField(Post, primary_key=True)
    sample_width = UnsignedIntegerField()
    sample_height = UnsignedIntegerField()

    preview_width = UnsignedSmallIntegerField()
    preview_height = UnsignedSmallIntegerField()

@create_table
class ImageURL(MyModel):
    post = ForeignKeyField(Post, primary_key=True)
    img_url = CharField()
    sample_url = CharField()
    preview_url = CharField()

@create_table
class Status(MyModel):
    id = TinyIntegerAutoField(primary_key=True)
    value = CharField(unique=True)

@create_table
class PostStatus(MyModel):
    post = ForeignKeyField(Post, primary_key=True)
    status = ForeignKeyField(Status)

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
    id = TinyIntegerAutoField(primary_key=True)
    name = CharField(unique=True)
    ext = CharField(index=True, max_length=16)

@create_table
class Content(MyModel):
    post = ForeignKeyField(Post, backref='content', primary_key=True)
    sha256_current = FixedCharField(unique=True, max_length=64)
    sha256_when_acquired = FixedCharField(unique=True, null=True, max_length=64)
    mimetype = ForeignKeyField(MimeType, backref='contents')
    file_size_current = IntegerField(index=True)
    file_size_when_acquired = IntegerField(index=True, null=True)
    we_modified_it = BooleanField(index=True, default=False)

@create_table
class ContentThumbnail(MyModel):
    content = ForeignKeyField(Content, backref='thumbnail', primary_key=True)
    generated_at = DateTimeField(default=datetime.datetime.now, index=True)
    mimetype = ForeignKeyField(MimeType)


class ContentOld(MyModel):
    post = ForeignKeyField(Post, backref='content')
    ext = CharField(index=True, null=True, max_length=16)
    sha256_current = FixedCharField(unique=True, max_length=64)
    sha256_when_acquired = FixedCharField(unique=True, null=True, max_length=64)
    mimetype = ForeignKeyField(MimeType, backref='contents')
    file_size_current = IntegerField(index=True)
    file_size_when_acquired = IntegerField(index=True, null=True)
    we_modified_it = BooleanField(index=True, default=False)

@create_table
class Comment(MyModel):
    id = UnsignedAutoField(primary_key=True)
    post = ForeignKeyField(Post, backref='comments')
    local_id = UnsignedIntegerField(index=True)
    body = TextField()
    creator = ForeignKeyField(User, backref='comments')

    score = IntegerField(index=True, null=True)
    comment_created_at = DateTimeField()
    row_created_at = DateTimeField(default=datetime.datetime.now)
    row_updated_at = DateTimeField(default=datetime.datetime.now)
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
    id = SmallIntegerAutoField(primary_key=True)
    name = CharField(unique=True)

@create_table
class QueuedImportEntity(MyModel):
    id = UnsignedAutoField(primary_key=True)
    board = Board('queued_import_entities')
    entity_type = ForeignKeyField(EntityType)
    entity_local_id = UnsignedIntegerField()
    additional_data = TextField(null=True)
    enqueued_at = DateTimeField(default=datetime.datetime.now, index=True)
    errors_encountered = UnsignedIntegerField(null=True, index=True)
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
    id = UnsignedAutoField(primary_key=True)
    queued_entity = ForeignKeyField(QueuedImportEntity, index=True, on_delete='CASCADE')
    when = DateTimeField(default=datetime.datetime.now)
    error = TextField()

@create_table
class ImportedEntity(MyModel):
    id = UnsignedAutoField(primary_key=True)
    board = Board('imported_entities')
    entity_type = ForeignKeyField(EntityType)
    entity_local_id = UnsignedIntegerField(index=True)
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

@create_table
class Note(MyModel):
    id = UnsignedAutoField(primary_key=True)
    board = Board('notes')
    local_id = UnsignedIntegerField(index=True)
    author = ForeignKeyField(User, backref='notes')
    post = ForeignKeyField(Post, backref='notes')
    body = TextField()
    version = SmallIntegerField()
    
    note_created_at = DateTimeField()
    note_updated_at = DateTimeField()

    row_created_at = DateTimeField(default=datetime.datetime.now)

    is_active = BooleanField()

    x = UnsignedIntegerField()
    y = UnsignedIntegerField()
    width = UnsignedIntegerField()
    height = UnsignedIntegerField()
    
    class Meta:
        indexes = (
                (('board', 'local_id', 'version'), True), 
                (('board', 'local_id'), False), 
                )
