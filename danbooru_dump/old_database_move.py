from old_database import *
import sys
import random
sys.path.append('..')
from database import *
import traceback

ll = list(range(256))
if input('Y for unordered, anything else for ordered: ').lower() == 'y': random.shuffle(ll)
for n in ll:
    got_row = True
    while got_row:
        got_row = False
        print(n)
        db = get_content_db(hex(n)[2:].zfill(2))
        with db.bind_ctx((OldFile,)):
            try:
                row = OldFile.select().get()
            except:
                #traceback.print_exc()
                continue
            got_row = True
            print(row.sha256, row.mimetype, len(row.content))
            File.save_file(row.content, raise_if_exists=False, but_check_for_same=True)
            row.delete_instance()
    get_content_db(hex(n)[2:].zfill(2)).execute_sql('vacuum;')
