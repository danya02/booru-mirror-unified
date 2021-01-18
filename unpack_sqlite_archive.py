import os
from peewee import *
import random

class File(Model):
    name = CharField(primary_key=True)
    content = BlobField()

os.chdir('/hugedata/booru_old/rule34.xxx')

files = []
for i in os.popen('find -type f'):
    files.append(i.strip())

random.shuffle(files)

for f in files:
    db = SqliteDatabase(f)
    with File.bind_ctx(db):
        get = True
        while get:
            get = False
            file_row = File.get_or_none()
            if file_row:
                get = True
                print(file_row.name)
                with open('../rule34.xxx-files/' + file_row.name[:2] + '/' + file_row.name, 'wb') as handle:
                    print(handle.write(file_row.content))

                file_row.delete_instance()
    db.execute_sql('vacuum;')
