import sys
sys.path.append('..')
from database import *
from old_database import *
import traceback

iterated = True
loop_count = 1190
while iterated:
    print('===========')
    print('= PAGE '+str(loop_count) + ' =')
    print('===========')
    print()

    iterated = False
    for row in Content.select().limit(10).offset(loop_count*10).iterator():
        iterated = True
        print('=====')
        name = row.sha256_current
        print(name, repr(row))
        try:
            content_row = OldFile.get_file_by_sha256(name, return_instance=True)
        except OldFile.DoesNotExist:
            print('old does not exist')
            continue
        print('old is', repr(content_row))
        try:
            File.save_file(content_row.content)
        except:
            traceback.print_exc()
            if input('still save? y/n:')=='y':
                File.save_file(content_row.content, False)
        print('deleted', OldFile.delete_file_by_sha256(name), 'rows')
    loop_count += 1
