import sqlite3
import os
import stat

'''
zfsobj   [zfsobj_id, uid, gid, ..., size, trusted_lov, trusted_link]
name     [fid, name, parent_fid, pk(name, parent_fid), index(fid)]
metadata [fid, uid, gid, ..., size]

todo: make names match the above
todo: integrate with zester

get_entries_for_path(path):
    fid=path_to_fid()
    # recur into .. join on mode in metadata .. basically listdir

get_entries_for_uid_in_path(uid, path):
    entries = []
    for fid in get_fids_for_uid(uid):
       for path in fid_to_path(fid):
          etnries.append(path)
'''


def create_names_table(conn):
    conn.execute('''
        create table name (
          fid        text,
          name       text,
          parent_fid text,
          primary key (name, parent_fid)
          )''')
    conn.execute('create index name_f on name (fid)')
    conn.execute('create index name_p on name (parent_fid)')


def drop_names_table(conn):
    conn.execute("drop table name")


def insert_name(curs, fid, name, parent_fid):
    curs.execute(
        "insert into name (fid, name, parent_fid) values (?, ?, ?)",
        [fid, name, parent_fid]).fetchone()


def populate_names(curs, parent_dir, parent_id):
    for entry in os.listdir(parent_dir):
        path_entry = os.path.join(parent_dir, entry)
        try:
            st = os.stat(path_entry)
            insert_name(curs, st.st_ino, entry, parent_id)
            if stat.S_ISDIR(st.st_mode):
                populate_names(curs, path_entry, st.st_ino)
        except OSError:
            pass


def fid_to_path(conn, srch_fid):
    def helper(fid0, sofar):
        sql = "select * from name where fid = ?"
        (fid, name, parent_fid) = conn.execute(sql, [fid0]).fetchone()
        sofar.append(name)
        if parent_fid is None:
            return sofar
        else:
            return helper(parent_fid, sofar)

    ls0 = helper(srch_fid, [])
    ls0.reverse()
    path_build = ""
    for name0 in ls0:
        path_build = path_build + "/" + name0
    return path_build


def dump(cur):
    for row in cur:
        print(row)


def test_names():
    conn = sqlite3.connect(":memory:")
    create_names_table(conn)
    curs = conn.cursor()
    populate_names(curs, "test_dir", None)
    conn.commit()
    sql = """select * from name where name like ? limit 100"""
    curs = conn.execute(sql, ["file2.txt"])
    (fid, _, _) = curs.fetchone()
    assert fid_to_path(conn, fid) == '/a/ab/file2.txt'
    conn.close()


if __name__ == '__main__':
    test_names()
