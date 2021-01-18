from database import *

#boards = list(Imageboard.select())

last_tag_id = TagPostCount.select(fn.MAX(TagPostCount.tag_id)).scalar() or 1

for tag in Tag.select().where(Tag.id > last_tag_id).order_by(Tag.id).limit(5000).iterator():
    counts = PostTag.select(Post.board_id, fn.COUNT(1)).join(Post).where(PostTag.tag == tag).group_by(Post.board).tuples()
    with db.atomic():
        for board, count in counts:
            board = Imageboard.get_by_id(board)
            print(board.name, tag.name, count)
            if count != 0:
                TagPostCount.create(tag=tag, post_count=count, board=board)
