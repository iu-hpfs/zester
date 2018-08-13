from __future__ import print_function
import sqlite3
import os
import stat


def drop_name_table(conn):
    conn.execute("drop table if exists names")


def create_name_table(conn):
    conn.execute('''
        create table names (
          fid        text,
          name       text,
          parent_fid text,
          primary key (name, parent_fid)
          )''')
    conn.execute('create index name_f on names (fid)')
    conn.execute('create index name_p on names (parent_fid)')


def setup_name_table(conn):
    drop_name_table(conn)
    create_name_table(conn)


def insert_name(curs, fid, name, parent_fid):
    curs.execute("insert into names (fid, name, parent_fid) values (?, ?, ?)",
                 [fid, name, parent_fid])


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

def clean_remote_dirs(conn):
    import re

    # Search through the 'names' table for virtual remote directory objects that connect child directories and files
    # to parent directories. Connect child objects to parent directories, and remove virtual directory objects from
    # table. These remote directory objects have filenames like [FID]:<MDT IDX>, where FID is the remote object FID,
    # and <MDT IDX> is a single digit index, matching the MDT index on which this object is located.

    # Create a regular expression to match internal Lustre names for
    # remote directory objects with their own FIDS.
    # remote_directory_object = re.compile(r'\[0x[0-9a-fA-F]+:0x[0-9a-fA-F]+:0x[0-9a-fA-F]+\]:[0-9]+')

    # Search 'names' table for objects with names matching the expected SQL regexp '[0x%:0x%:0x%]:_'.
    # Keep track of rowid for matching rows, so they be easily cleaned up.

    cur1 = conn.cursor()
    cur2 = conn.cursor()
    cur3 = conn.cursor()

    rows = cur1.execute("select rowid, fid, name, parent_fid from names where name like '[0x%:0x%:0x%]:_'")

    # Search 'names' table for objects with parent_fid equal to remote directory FID. Double-check that FID matches
    # the filename regexp '[FID]:[0-9]'.

    for row in rows:
        (rowid, fid, name, parent_fid) = row

        if re.match("\[{fid:s}\]:[0-9]".format(fid=fid), name):
            children = cur2.execute("select rowid from names where parent_fid = ?", [fid])

            for child_row in children:
                child_rowid = child_row[0]
                cur3.execute("update names set parent_fid = ? where rowid = ?", [parent_fid, child_rowid])

        cur2.execute("delete from names where rowid = ?", [rowid])

    conn.commit()

    cur1.close()
    cur2.close()
    cur3.close()

def fid_to_path(conn, srch_fid):
    import re

    def helper(cur, fid0, path_so_far):
        sql = "select fid, name, parent_fid from names where fid = ?"
        search_output = cur.execute(sql, [fid0]).fetchone()

        # FWIW: [0x200000007:0x1:0x0] is the FID for the root of the lustre filesystem. We could iterate until
        # we obtain that for the parent FID. However, the null search works as well, and will not cause an infinite
        # loop if we have an incomplete path in the database.
        #
        # Remote directories will have a filename that includes a FID with the []:x notation, where the FID is
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
        # We don't want these non-Unix path components to show in the output.
        # So, we'll look for them and squeeze them out of the returned path.
        #

        if search_output is None:
            # Need to figure out how to deal with cur.execute.fetchone() returning null results
            return path_so_far
        else:
            (fid, name, parent_fid) = search_output
            path_so_far.append(name)
            return helper(cur, parent_fid, path_so_far)

    # Initialize an empty list for paths to the srch_fid, which will be appended to below.
    paths = []
    cur = conn.cursor()

    # Do the first search outside the helper() function,
    # to account for possible multiple hard links with same srch_fid.
    sql = "select fid, name, parent_fid from names where fid = ?"
    search_output_list = cur.execute(sql, [srch_fid]).fetchall()
    for search_output in search_output_list:
        (fid, name, parent_fid) = search_output
        ls0 = helper(cur, parent_fid, [name])

        # Build the path based on the search for the current parent_fid
        # Note: Multiple paths to a FID implies multiple hard links to this FID, which will
        # have different paths, and possibly different parent fids. So, we loop over each
        # case.
        ls0.reverse()
        path_build = ""
        for name0 in ls0:
            path_build = path_build + "/" + name0

        # Add the reconstructed path to the list of paths to return
        paths.append(path_build)
    cur.close()

    # Return all paths to the srch_fid
    return paths

def path_to_fid(conn, srch_path):
    import os.path

    # Note that the value of the root (/) FID sequence number in the Lustre 2.x filesystem
    # (located on MDT0) is 0x200000007. We'll need this to know where to start, as we pull
    # picking our way through the path.
    #
    # See Lustre source file: include/lustre/lustre_idl.h
    #     /**
    #  * Note that reserved SEQ numbers below 12 will conflict with ldiskfs
    #  * inodes in the IGIF namespace, so these reserved SEQ numbers can be
    #  * used for other purposes and not risk collisions with existing inodes.
    #  *
    #  * Different FID Format
    #  * http://arch.lustre.org/index.php?title=Interoperability_fids_zfs#NEW.0
    #  */
    # enum fid_seq {
    # ...
    # FID_SEQ_ROOT            = 0x200000007ULL,  /* Located on MDT0 */
    # ...
    #
    # Note that the referenced link is currently only available at:
    # http://wiki.old.lustre.org/index.php/Architecture_-_Interoperability_fids_zfs

    path_list = []
    head = srch_path
    while head is not '':
        head, tail = os.path.split(head)
        path_list.append(tail)

    path_list.reverse()
    cur = conn.cursor()

    # Start parent fid at the root FID
    parent_fid = '0x200000007:0x1:0x0'
    sql = "select fid from names where parent_fid = ? and name = ?"

    for filename in path_list:
        search_output = cur.execute(sql, [parent_fid, filename]).fetchone()
        if search_output is not None:
            parent_fid = search_output[0]
        else:
            parent_fid = ''
            print('ERROR: No FID found for path: {0:s}'.format(srch_path),end='')
            break

    fid = parent_fid

    return fid

def dump(cur):
    for row in cur:
        print(row)


def test_names():
    conn = sqlite3.connect(":memory:")
    setup_name_table(conn)
    curs = conn.cursor()
    populate_names(curs, "test_dir", None)
    conn.commit()
    sql = """select * from names where name like ? limit 100"""
    curs = conn.execute(sql, ["file2.txt"])
    (fid, _, _) = curs.fetchone()
    assert fid_to_path(conn, fid) == '/a/ab/file2.txt'
    conn.close()


if __name__ == '__main__':
    test_names()
