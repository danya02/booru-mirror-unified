import redis
import time
import sys
sys.path.append('..')
from database import *
import magic
import os

main = redis.Redis()
cache = redis.Redis(db=2)
also_save_to_db = True

os.chdir('/hugedata/booru/danbooru2019/danbooru2019/original')

def sha256():
    for i in range(1000):
        handle = os.popen('sha256sum 0'+str(i).zfill(3)+'/*')
        for i in handle:
            a,b = i.split()
            yield os.getcwd() + '/' + b, a

def mimetype():
    for i in range(1000):
        handle = os.popen('mimetype 0'+str(i).zfill(3)+'/*')
        for i in handle:
            print(i)
            a,b = i.strip().split(': ')
            yield os.getcwd() + '/' + a, b

s = sha256()
mt = mimetype()

ss = dict()
mts = dict()

def add_to_db(name):
    sha = ss.pop(name)
    mime = mts.pop(name)
    if main.get(name) is not None:
        cache.set(name, sha+' '+mime)
        print(cache.dbsize())
    else:
        print('NOT QUEUED ALREADY:',name)

while 1:
    nextsf, nextsv = next(s)
    print(nextsf, nextsv)
    ss.update({nextsf: nextsv})
    if nextsf in mts:
        add_to_db(nextsf)


    nextmtf, nextmtv = next(mt)
    print(nextsf, nextsv)
    mts.update({nextmtf: nextmtv})
    if nextmtf in ss:
        add_to_db(nextmtf)

