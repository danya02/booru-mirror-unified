from database import *
import functools

class BooruDatabase:
    def __init__(self, booru):
        if not isinstance(booru, Imageboard):
            if isinstance(booru, str):
                booru = Imageboard.get(Imageboard.name == booru)
            else:
                booru = Imageboard.get_by_id(booru)
        else:
            pass
        self.booru = booru

    @property
    def post(self):
        return PostDatabase(self.booru)

    @property
    def tag(self):
        return TagDatabase(self.booru)

#    @property
#    def comment(self):
#        return CommentDatabase(self.booru)

    @property
    def user(self):
        return UserDatabase(self.booru)

class PostDatabase:
    def __init__(self, booru):
        self.booru = booru

    @functools.lru_cache(maxsize=32)
    def __getitem__(self, id):
        return Post.get_or_none(Post.local_id==id, Post.board==self.booru)

    def __setitem__(self, id, model):
        if not isinstance(model, Post):
            raise TypeError
        model.board = self.booru
        model.local_id = id
        try:
            model.save(force_insert=True)
        except IntegrityError:
            model.save()

class TagDatabase:
    def __init__(self, booru):
        pass

    @staticmethod
    def __getitem__(item):
        if isinstance(item, int) or isinstance(item, Post):
            if isinstance(item, int):
                item = Post.select(Post.id).where(Post.id == item).where(Post.board == self.booru).get()
            return set(
                  [i.name for i in Tag.select(Tag.name).join(PostTag).where(PostTag.post == item)]
		)
        elif isinstance(item, str):
            return Tag.get_or_none(Tag.name == item)
        else:
            raise TypeError

    @staticmethod
    @functools.lru_cache(maxsize=1024)
    def get_tag(name):
        try:
            return Tag.get(Tag.name == name)
        except Tag.DoesNotExist:
            return Tag.create(name=name)

    @staticmethod
    def __setitem__(item, value):
        if isinstance(item, Post):
            if isinstance(item, int):
                item = Post.select(Post.id).where(Post.id == item).where(Post.board == self.booru).get()
            if not isinstance(value, list): raise TypeError
            with db.atomic():
                subquery_tag_ids = PostTag.select(PostTag.tag_id).where(PostTag.post == item)
                #TagPostCount.update(post_count = TagPostCount.post_count - 1, changed_at=fn.Now()).where(TagPostCount.board == item.board).where(TagPostCount.tag.in_(subquery_tag_ids)).execute()

                PostTag.delete().where(PostTag.post == item).execute()

                post_tags = []
                for tagname in value:
                    post_tags.append(PostTag(post=item, tag=TagDatabase.get_tag(tagname)))
                #    TagPostCount.get_or_create(board=item.board, tag=TagDatabase.get_tag(tagname))
                try:
                    PostTag.bulk_create(post_tags)
                except IntegrityError:
                    for i in post_tags:
                        try:
                            i.save(force_insert=True)
                        except IntegrityError:
                            pass

                #TagPostCount.update(post_count = TagPostCount.post_count + 1, changed_at=fn.Now()).where(TagPostCount.board == item.board).where(TagPostCount.tag.in_(subquery_tag_ids)).execute()
        else:
            raise TypeError

class UserDatabase:
    def __init__(self, booru):
        self.booru = booru

    def __getitem__(self, item):
        if isinstance(item, int):
            return User.get_or_none(User.local_id==item, User.board==self.booru)
        else:
            raise TypeError

    def __setitem__(self, item, value):
        if not isinstance(item, int): raise TypeError
        if not isinstance(value, User): raise TypeError
        value.local_id = item
        value.board = self.booru
        try:
            value.save(force_insert=True)
        except IntegrityError:
            value.save()

