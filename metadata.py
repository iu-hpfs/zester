def setup_metadata_db(meta_db):
    meta_cur = meta_db.cursor()
    meta_cur.execute('drop table if exists metadata')
    meta_cur.execute('''
        create table metadata (fid text primary key, uid integer, gid integer, 
        ctime integer, mtime integer, atime integer, mode integer,
        size integer, obj_type text)
        ''')
    meta_cur.close()
    meta_cur = meta_db.cursor()
    meta_cur.execute('delete from metadata')
    meta_cur.close()


def save_metadata_obj(meta_cur, fid, uid, gid, ctime, mtime, atime, mode, size, obj_type):
    cmd = '''INSERT INTO [metadata] 
          (fid, uid, gid, ctime, mtime, atime, mode, size, obj_type)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    meta_cur.execute(cmd, (fid, uid, gid, ctime, mtime, atime, mode, size, obj_type))