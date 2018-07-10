import sys

def setup_metadata_db(meta_db):
    meta_cur = meta_db.cursor()
    meta_cur.execute('drop table if exists metadata')
    meta_cur.execute(
        'CREATE TABLE metadata (mdt_id INTEGER, path TEXT, uid INTEGER, gid INTEGER, ' +
        'ctime INTEGER, mtime INTEGER, atime INTEGER, mode INTEGER, type INTEGER, ' +
        'size INTEGER, fid TEXT, PRIMARY KEY (mdt_id, path))')
    meta_cur.close()
    meta_cur = meta_db.cursor()
    meta_cur.execute('delete from metadata')
    meta_cur.close()

def save_metadata_obj(meta_cur, mdt_id, path, uid, gid, ctime, mtime, atime, mode, type, size, fid):
    cmd = 'INSERT INTO [metadata] ' + \
          '(mdt_id, path, uid, gid, ctime, mtime, atime, mode, type, size, fid)' + \
          'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    try:
        meta_cur.execute(cmd, (mdt_id, path, uid, gid, ctime, mtime, atime, mode, type, size, fid))
    except:
        print("Unexpected error:", sys.exc_info()[0])
        print(cmd)
        raise
