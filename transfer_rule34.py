import database
from peewee import *


BOARD, _ = database.Imageboard.get_or_create(name='rule34', url='https://rule34.xxx')

olddb = MySQLDatabase('rule34', user='booru', password='booru')

class OldModel(Model):
    class Meta:
        database = olddb

def confirm_delete(row):
    print('OK to delete', repr(row), str(row), row.__dict__)
    return input('y/n> ').lower() == 'y'

class AccessLevel(OldModel):
    name = CharField(unique=True)

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
            database.UserAccessLevel.get_or_create(user=row, level= (database.AccessLevel.get_or_create(name=self.level.name))[0] )

        count = Post.select(fn.COUNT(1)).where(Post.creator == self).scalar() + Comment.select(fn.COUNT(1)).where(Comment.author == self).scalar() + Note.select(fn.COUNT(1)).where(Note.author == self).scalar()
        if count == 0:
            if confirm_delete(self):
                self.delete_instance()

        return row

class Rating(OldModel):
    value = CharField(unique=True)

class Status(OldModel):
    value = CharField(unique=True)

class Tag(OldModel):
    name = CharField(unique=True)

class Type(OldModel):
    name = CharField(unique=True)

class TagPostCount(OldModel):
    tag = ForeignKeyField(Tag, primary_key=True)
    value = IntegerField()

class TagType(OldModel):
    tag = ForeignKeyField(Tag)
    type = ForeignKeyField(Type)
    
    def migrate(self):
        type, _ = database.Type.get_or_create(name=self.type.name)
        tag, _ = database.Tag.get_or_create(name=self.tag.name)
        database.TagType.get_or_create(type=type, tag=tag)
        if confirm_delete(self):
            TagType.delete().where(TagType.tag == self.tag).where(TagType.type == self.type).execute()

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
        post, _ = database.Post.get_or_create(board=BOARD, local_id=self.id, post_created_at=self.created_at, post_updated_at=self.changed_at, uploaded_by=self.creator.migrate(), souece=(self.source or None), rating=self.rating.value, score=self.score, parent_id=self.parent.id)
        
        database.ImageMetadata.get_or_create(post=post, image_width=self.width, image_height=self.height, md5=self.md5)
        database.PreviewSizeInfo.get_or_create(post=post, sample_width=self.sample_width, sample_height=self.sample_height, preview_width=self.preview_width, preview_height=self.preview_height)
        database.ImageURL.get_or_create(post=post, img_url=self.url, sample_url=self.sample, preview_url=self.preview)
        database.PostStatus.get_or_create(post=post, status=database.Status.get_or_create(name=self.status.value)[0])
        


class Content(OldModel):
    post = ForeignKeyField(Post, backref='content')
    path = CharField(unique=True)
    original_length = IntegerField()
    current_length = IntegerField()


class PostTag(OldModel):
    tag = ForeignKeyField(Tag, backref='posts')
    post = ForeignKeyField(Post, backref='tags')
    class Meta:
        primary_key = CompositeKey('tag', 'post')

class Comment(OldModel):
    author = ForeignKeyField(User, backref='comments')
    post = ForeignKeyField(Post, backref='comments')
    id = IntegerField(primary_key=True, unique=True)
    body = TextField()
    score = IntegerField()
    created_at = DateTimeField()

class Note(OldModel):
    id = IntegerField(primary_key=True, unique=True)
    author = ForeignKeyField(User, backref='comments')
    post = ForeignKeyField(Post, backref='comments')
    body = TextField()
    created_at = DateTimeField()
    updated_at = DateTimeField()
    is_active = BooleanField()
    version = IntegerField()

    x = IntegerField()
    y = IntegerField()
    width = IntegerField()
    height = IntegerField()

board = Imageboard.get_or_create(name='rule34', base_url='https://rule34.xxx')


