#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
    for columns from TABLE where 'username' is non-ascii
        replace 'username' with uuid.uuid4()
'''
import MySQLdb as mdb
import uuid


FILE_NAME = '/tmp/test.tmp'
FILE_MODE = 'w' 
FETCH_SIZE = 10000
TABLE_NAME = 'users'
FIELD_NAME = 'user'
MYSQL_DB  = ''

def update_field(filename=FILE_NAME, filemode=FILE_MODE, size=FETCH_SIZE,
     tablename=TABLE_NAME, fieldname=FIELD_NAME):
    affected_rows = 0
    con = mdb.connect(*MYSQL_DB)
    with open(filename, filemode) as f:
        with con:
            cur = con.cursor(mdb.cursors.DictCursor)
            cur.execute('select * from %s;' % tablename)
            while True:
                itemList = cur.fetchmany(size)
                if not itemList:
                    break
                for item in itemList:
                    try:
                        item[fieldname].strip().decode('ascii')
                    except UnicodeDecodeError, e:
                        affected_rows += 1
                        username = '%s%s'.encode('utf-8') % ('USER', uuid.uuid4().hex[0:20].upper())
                        cur.execute('UPDATE %s SET %s="%s" WHERE id=%ld;' % (
                           tablename, fieldname, username, user['id']))
                        f.writelines('%s,%s,%s' % (
                            user['id'], user[fieldname], username))
                con.commit()
                f.flush()
    return affected_rows

def check_field(size=FETCH_SIZE, tablename=TABLE_NAME, fieldname=FIELD_NAME):
    affected_rows = 0
    con = mdb.connect(*MYSQL_DB)
    with con:
        cur = con.cursor(mdb.cursors.DictCursor)
        cur.execute('select * from %s;' % tablename)
        while True:
            itemList = cur.fetchmany(size)
            if not itemList:
                break
            for item in itemList:
                try:
                    item[fieldname].strip().decode('ascii')
                except UnicodeDecodeError, e:
                    affected_rows += 1
    return affected_rows

if __name__ == '__main__':
    print '>>>> update %s["%s"]' % (TABLE_NAME, FIELD_NAME)
    print '%ld rows affected <<<<' % check_field()
