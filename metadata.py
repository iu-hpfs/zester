def setup_metadata_db(meta_db):
    meta_cur = meta_db.cursor()
    meta_cur.execute('drop table if exists metadata')
    meta_cur.execute('''
        CREATE TABLE metadata (fid TEXT primary key, uid INTEGER, gid INTEGER, 
        ctime INTEGER, mtime INTEGER, atime INTEGER, mode INTEGER,
        size INTEGER)
        ''')
    meta_cur.close()
    meta_cur = meta_db.cursor()
    meta_cur.execute('delete from metadata')
    meta_cur.close()


def save_metadata_obj(meta_cur, fid, uid, gid, ctime, mtime, atime, mode, size):
    cmd = '''INSERT INTO [metadata] 
          (fid, uid, gid, ctime, mtime, atime, mode, size)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)'''
    meta_cur.execute(cmd, (fid, uid, gid, ctime, mtime, atime, mode, size))