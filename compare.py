#!/usr/bin/env python2

# Copyright [2017], The Trustees of Indiana University. Licensed under the
# GNU General Public License Version 2 (see included COPYING.TXT). You
# may not use this file except in compliance with the License. Unless
# required by applicable law or agreed to in writing, software distributed
# under the  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, either express or implied. See the terms and
# conditions of the GPL2 License for more details.
#
# Written by Kenrick Rawlings & Shawn Slavin for the High Performance File
# Systems group in the Pervasive Technology Institute at Indiana University.

import sqlite3
import sys

def doCompare(zester_db_fname,posix_db_fname, prefix):
    print('doCompare', (zester_db_fname, posix_db_fname, prefix))
    zester_db = sqlite3.connect(zester_db_fname, isolation_level=None)
    zester_db.text_factory = str
    posix_db = sqlite3.connect(posix_db_fname, isolation_level=None)
    posix_db.text_factory = str

    match_file = open("match.txt", "w")
    posix_only_file = open("posix_only.txt", "w")
    zester_only_file = open("zester_only.txt", "w")

    query = "SELECT path, fid, atime, ctime, mtime, uid, gid, mode, size, inode, objects, project, type FROM [metadata] ORDER BY PATH"

    zester_cursor = zester_db.cursor()
    zester_cursor.execute(query)

    posix_cursor = posix_db.cursor()
    posix_cursor.execute(query)

    zester_curr_row = zester_cursor.fetchone()
    posix_curr_row = posix_cursor.fetchone()

    count = 0
    matchCount = 0
    posixCount = 0
    zesterCount = 0
    while (zester_curr_row != None) or (posix_curr_row != None):
        count = count + 1
        if zester_curr_row == None:
            posixCount = posixCount + 1
            posix_only_file.write(str(posix_curr_row))
            posix_only_file.write("\n")
            posix_curr_row = posix_cursor.fetchone()
        elif posix_curr_row == None:
            zesterCount = zesterCount + 1
            zester_only_file.write(str(zester_curr_row))
            zester_only_file.write("\n")
            zester_curr_row = zester_cursor.fetchone()
        else:
            zester_curr_path = prefix + zester_curr_row[0]
            posix_curr_path = posix_curr_row[0]
            if zester_curr_path == posix_curr_path:
                (zester_path, zester_fid, zester_atime, zester_ctime, zester_mtime, zester_uid, zester_gid, zester_mode, zester_size, zester_inode, zester_objects, zester_project, zester_type) = zester_curr_row
                (posix_path, posix_fid, posix_atime, posix_ctime, posix_mtime, posix_uid, posix_gid, posix_mode, posix_size, posix_inode, posix_objects, posix_project, posix_type) = posix_curr_row
                match_file.write(zester_curr_path + " ")
                if abs(int(zester_atime) - int(posix_atime)) > 2:
                    match_file.write(" (!!atime: Z: " + str(zester_atime) + " P: " + str(posix_atime) + ")")
                if abs(int(zester_mtime) - int(posix_mtime)) > 2:
                    match_file.write(" (!!mtime: Z: " + str(zester_mtime) + " P: " +  str(posix_mtime) + ")")
                if abs(int(zester_ctime) - int(posix_ctime)) > 2:
                    match_file.write(" (!!ctime: Z: " + str(zester_ctime) + " P: " + str(posix_ctime) + ")")
                if str(zester_uid) != str(posix_uid):
                    match_file.write(" (!!uid: Z: " + str(zester_uid) + " P: " + str(posix_uid) + ")")
                if str(zester_gid) != str(posix_gid):
                    match_file.write(" (!!gid: Z: " + str(zester_gid) + " P: " + str(posix_gid) + ")")
                if str(zester_size) != str(posix_size):
                    match_file.write(" (!!size: Z: " + str(zester_size) + " P: " + str(posix_size) + ")")
                match_file.write("\n")
                matchCount = matchCount + 1
                zester_curr_row = zester_cursor.fetchone()
                posix_curr_row = posix_cursor.fetchone()
            elif zester_curr_path < posix_curr_path:
                zesterCount = zesterCount + 1
                zester_only_file.write(str(zester_curr_row))
                zester_only_file.write("\n")
                zester_curr_row = zester_cursor.fetchone()
            else:
                posixCount = posixCount + 1
                posix_only_file.write(str(posix_curr_row))
                posix_only_file.write("\n")
                posix_curr_row = posix_cursor.fetchone()
    print('count: ', count)
    print('matchCount: ', matchCount)
    print('posixCount: ', posixCount)
    print('zesterCount: ', zesterCount)
    zester_cursor.close()
    zester_db.close()

    posix_cursor.close()
    posix_db.close()

    match_file.close()
    posix_only_file.close()
    zester_only_file.close()

zesterDbFname = 'metadata.db'
posixDbFname  = 'trystat.db'

# '/mnt/zester/'

if __name__ == '__main__':
    doCompare(zesterDbFname, posixDbFname, sys.argv[1])
