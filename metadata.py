import sys

def setup_metadata_db(meta_db):
    meta_cur = meta_db.cursor()
    meta_cur.execute('drop table if exists metadata')
    meta_cur.execute(
        'CREATE TABLE metadata (fid TEXT, uid INTEGER, gid INTEGER, ' +
        'ctime INTEGER, mtime INTEGER, atime INTEGER, mode INTEGER, type INTEGER, ' +
        'size INTEGER, PRIMARY KEY (fid))')
    meta_cur.close()
    meta_cur = meta_db.cursor()
    meta_cur.execute('delete from metadata')
    meta_cur.close()

def save_metadata_obj(meta_cur, fid, uid, gid, ctime, mtime, atime, mode, type, size):
    cmd = 'INSERT INTO [metadata] ' + \
          '(fid, uid, gid, ctime, mtime, atime, mode, type, size)' + \
          'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
    try:
        meta_cur.execute(cmd, (fid, uid, gid, ctime, mtime, atime, mode, type, size))
    except:
        print("Unexpected error:", sys.exc_info()[0])
        print(cmd)
        raise
