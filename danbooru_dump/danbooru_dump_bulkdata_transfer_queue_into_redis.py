import sys
sys.path.append('..')
from database import *
from danbooru_dump_bulkdata_transfer import FileToImportTMP  
import sys

current_page = 0
while 1:
    print(current_page, file=sys.stderr)
    for row in FileToImportTMP.select().paginate(current_page, 1000).iterator():
        print(f'SET "{row.path}" "0"')
    current_page += 1
