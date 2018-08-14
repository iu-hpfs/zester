#!/usr/bin/env python2

# Copyright [2017], The Trustees of Indiana University. Licensed under the
# GNU General Public License Version 2 (see COPYING.TXT). You may not use
# this file except in compliance with the License. Unless required by
# applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the terms and
# conditions of the GPL2 License for more details.
#
# Written by Shawn Slavin for the High Performance File
# Systems group in the Pervasive Technology Institute at Indiana University.
#
# Some code for recursive, pickled stats for tests with Zester
# code SDS, 2017-02-01

from __future__ import print_function, division
import os
import stat
import sqlite3
import sys


def get_filetype(mode):
    if stat.S_ISREG(mode):
        filetype = 'f'
    elif stat.S_ISDIR(mode):
        filetype = 'd'
    elif stat.S_ISLNK(mode):
        filetype = 'l'
    elif stat.S_ISFIFO(mode):
        filetype = 'p'
    elif stat.S_ISBLK(mode):
        filetype = 'b'
    elif stat.S_ISCHR(mode):
        filetype = 'c'
    elif stat.S_ISSOCK(mode):
        filetype = 's'
    else:
        filetype = 'unknown'
        print('Error in get_filetype(): '
              'Could not determine filetype for octal mode '
              '{0:#o}'.format(mode))
    return (filetype)


def make_stats(topdir, dbPath):
    conn = sqlite3.connect(dbPath, isolation_level=None)
    conn.text_factory = str
    c = conn.cursor()
    c.execute('drop table if exists metadata')
    c.execute('''create table metadata (
              path TEXT PRIMARY KEY,
              uid INTEGER,
              gid INTEGER,
              ctime INTEGER,
              mtime INTEGER,
              atime INTEGER,
              mode INTEGER,
              size INTEGER)''')
    for subdir, dirs, files in os.walk(topdir):
        i = 0
        # stat files and directories by iterating over files and dirs lists
        for item in files + dirs:
            path = subdir + os.sep + item
            print(path)
            # use os.lstat(), which does *not* follow symbolic links
            st = os.lstat(path)
            uid = st.st_uid
            gid = st.st_gid
            ctime = int(st.st_ctime)
            mtime = int(st.st_mtime)
            atime = int(st.st_atime)
            mode = st.st_mode
            size = st.st_size
            i = i + 1
            c.execute('insert into metadata values(?,?,?,?,?,?,?,?)',
                      (path, uid, gid, ctime, mtime, atime, mode, size))
    conn.commit()
    conn.close()


if __name__ == '__main__':
    make_stats('.', '/tmp/stats.db')
# os.getcwdu()