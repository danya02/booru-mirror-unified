from peewee import *
import hashlib
import os

content_databases = dict()

IMAGE_DIR_OLD = '/hugedata/booru_old/unified'

def sha256_hash(data):
    hasher = hashlib.sha256()
    hasher.update(data)
    return hasher.hexdigest()

def get_content_db(name):
    database = content_databases.get( name[:2] )
    if database is None:
        database = SqliteDatabase(IMAGE_DIR_OLD+'/'+name[:2]+'.db', timeout=300, pragmas={'secure_delete': 'off', 'auto_vacuum': 1})
        print(IMAGE_DIR_OLD+'/'+name[:2]+'.db')
    content_databases[ name[:2] ] = database
    return database

class OldFile(Model):
    class Meta:
        table_name = 'file'

    sha256 = CharField(unique=True, primary_key=True)
    mimetype = CharField()
    content = BlobField()

    @staticmethod
    def get_file_by_sha256(name, return_instance=False):
        database = get_content_db(name)
        with database.bind_ctx((OldFile,)):
            database.create_tables((OldFile,))
            file = OldFile.get(OldFile.sha256==name)
        if not return_instance:
            return file.content
        else:
            return file

    @staticmethod
    def delete_file_by_sha256(name):
        database = get_content_db(name)
        with database.bind_ctx((OldFile,)):
            return OldFile.delete().where(OldFile.sha256==name).execute()

