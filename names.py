import sqlite3
import os
import stat


def create_names_table(conn):
    conn.execute('''
        create table name (
          name_id    integer primary key autoincrement ,
          dataset_id integer not null,
          fid        text,
          name       text,
          parent_fid text,
          unique (name_id, dataset_id)
          )''')
    conn.execute('create index name_np on name (name_id, parent_fid)')
    conn.execute('create index name_f on name (fid)')
    conn.execute('create index name_p on name (parent_fid)')


def drop_names_table(conn):
    conn.execute("drop table name")


def insert_name(curs, dataset_id, fid, name, parent_fid):
    curs.execute("insert into name (dataset_id, fid, name, parent_fid) values (?, ?, ?, ?)",
                 [dataset_id, fid, name, parent_fid]).fetchone()


def populate_names(curs, dataset_id, parent_dir, parent_id):
    for entry in os.listdir(parent_dir):
        path_entry = os.path.join(parent_dir, entry)
        try:
            st = os.stat(path_entry)
            insert_name(curs, dataset_id, st.st_ino, entry, parent_id)
            if stat.S_ISDIR(st.st_mode):
                populate_names(curs, dataset_id, path_entry, st.st_ino)
        except OSError:
            pass


def fid_to_path(conn, srch_fid):
    def helper(fid0, sofar):
        sql = "select * from name where fid = ?"
        (_, _, fid, name, parent_fid) = conn.execute(sql, [fid0]).fetchone()
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
    dataset_id = 1
    populate_names(curs, dataset_id, "test_dir", None)
    conn.commit()
    sql = """select * from name where name like ? limit 100"""
    curs = conn.execute(sql, ["file2.txt"])
    (_, _, fid, _, _) = curs.fetchone()
    assert fid_to_path(conn, fid) == '/a/ab/file2.txt'
    conn.close()
