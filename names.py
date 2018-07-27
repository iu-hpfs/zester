import sqlite3
import os
import stat


def drop_name_table(conn):
    conn.execute("drop table if exists name")


def create_name_table(conn):
    conn.execute('''
        create table name (
          fid        text,
          name       text,
          parent_fid text,
          primary key (name, parent_fid)
          )''')
    conn.execute('create index name_f on name (fid)')
    conn.execute('create index name_p on name (parent_fid)')


def setup_name_table(conn):
    drop_name_table(conn)
    create_name_table(conn)


def insert_name(curs, fid, name, parent_fid):
    curs.execute("insert into name (fid, name, parent_fid) values (?, ?, ?)",
                 [fid, name, parent_fid])           # .fetchone()         Do we need the .fetchone() here?


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
    def helper(cur, fid0, path_so_far):
        sql = "select fid, name, parent_fid from name where fid = ?"
        search_output = cur.execute(sql, [fid0]).fetchone()
        # FWIW: [0x200000007:0x1:0x0] is the FID for the root of the lustre filesystem. We could iterate until
        # we obtain that for the parent FID. However, the null search works as well, and will not cause an infinite
        # loop if we have an incomplete path in the database.
        #
        # Sharded directories will have a filename that includes a FID with the []:x notation, where the FID is
        # inside the [] brackets, as usual, and the :x extension with have x equal to the value of the MDT index
        # on which that path is hung. Running 'lfs fid2path [0x240000401:0x5:0x0]:0' for example will point to the
        # same directory name as [0x240000401:0x5:0x0], and to the name of the parent directory fid:
        # >>> names.fid_to_path(db,'0x240000402:0x4e92:0x0')
        # u'/striped/[0x240000401:0x5:0x0]:1/1GB.0'
        #
        # On the zester-mds01 client VM:
        #
        # [root@zester-mds01 ~]# lfs path2fid /mnt/zester/striped/*
        # /mnt/zester/striped/1GB.0: [0x240000402:0x4e92:0x0]
        # /mnt/zester/striped/1GB.1: [0x200000402:0x10a:0x0]
        # /mnt/zester/striped/1GB.2: [0x240000402:0x4e97:0x0]
        # /mnt/zester/striped/1GB.3: [0x200000402:0x10b:0x0]
        #
        # [root@zester-mds01 ~]# lfs path2fid /mnt/zester/striped
        # [0x200000402:0x105:0x0]
        #
        # [root@zester-mds01 ~]# lfs fid2path zester [0x240000401:0x5:0x0]:0
        # striped/
        #
        # [root@zester-mds01 ~]# lfs fid2path zester [0x240000401:0x5:0x0]:1
        # striped/
        #
        # [root@zester-mds01 ~] lfs fid2path zester 0x200000402:0x105:0x0
        # striped
        #
        # [root@zester-mds01 ~]# lfs path2fid --parents /mnt/zester/striped
        # [0x200000007:0x1:0x0]/striped
        # [root@zester-mds01 ~]# lfs fid2path zester 0x200000007:0x1:0x0
        # /
        #
        if search_output is None:
            # Need to figure out how to deal with cur.execute.fetchone() returning null results
            return path_so_far
        else:
            (fid, name, parent_fid) = search_output
            print(fid, name, parent_fid)
            path_so_far.append(name)
            return helper(cur, parent_fid, path_so_far)

    cur = conn.cursor()

    ls0 = helper(cur, srch_fid, [])
    ls0.reverse()
    path_build = ""
    for name0 in ls0:
        path_build = path_build + "/" + name0
    cur.close()
    return path_build


def dump(cur):
    for row in cur:
        print(row)


def test_names():
    conn = sqlite3.connect(":memory:")
    setup_name_table(conn)
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
