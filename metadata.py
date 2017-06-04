import sys

def setup_metadata_db(meta_db):
    meta_cur = meta_db.cursor()
    meta_cur.execute('drop table if exists metadata')
    meta_cur.execute(
        'CREATE TABLE metadata (path TEXT PRIMARY KEY, uid INTEGER, gid INTEGER, ' +
        'ctime INTEGER, mtime INTEGER, atime INTEGER, mode INTEGER, type INTEGER, ' +
        'size INTEGER, fid TEXT)')
    meta_cur.close()
    meta_cur = meta_db.cursor()
    meta_cur.execute('delete from metadata')
    meta_cur.close()

def save_metadata_obj(meta_cur, path, uid, gid, ctime, mtime, atime, mode, type, size, fid):
    cmd = 'INSERT INTO [metadata] ' + \
          '(path, uid, gid, ctime, mtime, atime, mode, type, size, fid)' + \
          'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    try:
        meta_cur.execute(cmd, (path, uid, gid, ctime, mtime, atime, mode, type, size, fid))
    except:
        print("Unexpected error:", sys.exc_info()[0])
        print(cmd)
        raise
