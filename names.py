import sqlite3
import os
import stat


def create_names_table(conn):
    conn.execute('''
        create table name (
          name_id    integer not null primary key,
          fid        text,
          name       text,
          parent_fid text)''')
    conn.execute('create index name_np on name (name_id, parent_fid)')
    conn.execute('create index name_f on name (fid)')
    conn.execute('create index name_p on name (parent_fid)')


def drop_names_table(conn):
    conn.execute("drop table name")


def insert_name(curs, fid, name, parent_fid):
    curs.execute("insert into name (fid, name, parent_fid) values (?, ?, ?)",
                 [fid, name, parent_fid]).fetchone()


def resolve(curs, entry_id, sofar):
    lkup_sql = "select parent_id, name from entry where name_id = ?"
    (parent_id, name) = curs.execute(lkup_sql, [entry_id]).fetchone()
    new_sofar = os.path.join(name, sofar)
    if parent_id == 0:
        return new_sofar
    else:
        return resolve(curs, parent_id, new_sofar)


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
        (_, fid, name, parent_fid) = conn.execute(sql, [fid0]).fetchone()
        if parent_fid is None:
            return sofar
        else:
            sofar.append(name)
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
    conn = sqlite3.connect("names.db")
    # drop_names_table(conn)
    create_names_table(conn)
    curs = conn.cursor()
    populate_names(curs, "test_dir", None)
    conn.commit()
    sql = """select * from name where name like ? limit 100"""
    curs = conn.execute(sql, ["file2.txt"])
    (_, fid, _, _) = curs.fetchone()
    print(fid_to_path(conn, fid))
    conn.close()


if __name__ == "__main__":
    test_names()
