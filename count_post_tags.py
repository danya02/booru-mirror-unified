from database import *

#boards = list(Imageboard.select())

#last_tag_id = TagPostCount.select(fn.MAX(TagPostCount.tag_id)).scalar() or 1

for tag in Tag.select().join(TagPostCount, JOIN.LEFT_OUTER).where(TagPostCount.tag.is_null(True)).orwhere(TagPostCount.changed_at < (datetime.datetime.now() - datetime.timedelta(days=5))).order_by(Tag.id).iterator():
    counts = PostTag.select(Post.board_id, fn.COUNT(1)).join(Post).where(PostTag.tag == tag).group_by(Post.board).tuples()
    with db.atomic():
        sumcount = 0
        for board, count in counts:
            board = Imageboard.get_by_id(board)
            print(board.name, tag.name, count)
            sumcount += count
            #if count != 0:
            TagPostCount.set(tag=tag, post_count=count, board=board)
        TagPostCount.set(tag=tag, post_count=sumcount, board=None)
