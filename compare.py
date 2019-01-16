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
import names
import stat


def check_time(time0, time1):
    return abs(int(time0) - int(time1)) <= 3700


def check_zester_to_posix(posix_db, zester_db):
    zester_cursor = zester_db.cursor()
    zester_query = "SELECT fid, uid, gid, ctime, mtime, atime, mode, size, obj_type FROM metadata ORDER BY fid"
    zester_count = 0
    zester_cursor.execute(zester_query)
    zester_curr_row = zester_cursor.fetchone()
    while zester_curr_row:
        zester_count = zester_count + 1
        (zester_fid, zester_uid, zester_gid, zester_ctime, zester_mtime, zester_atime, zester_mode,
         zester_size, zester_type) = zester_curr_row
        for zester_path in names.fid_to_path(zester_db, zester_fid):
            posix_search_path = zester_path.replace('/ROOT/', '/mnt/td/', 1)
            posix_cursor = posix_db.cursor()
            posix_query = "SELECT uid, gid, ctime, mtime, atime, mode, size FROM metadata where path = ?"
            posix_cursor.execute(posix_query, [posix_search_path])
            posix_curr_row = posix_cursor.fetchone()
            try:
                (posix_uid, posix_gid, posix_ctime, posix_mtime, posix_atime, posix_mode, posix_size) = posix_curr_row
                if not check_time(posix_atime, zester_atime):
                    print('!atime', posix_search_path, zester_fid, posix_atime, zester_atime)
                if not check_time(posix_ctime, zester_ctime):
                    print('!ctime', posix_search_path, zester_fid, posix_ctime, zester_ctime)
                if not check_time(posix_mtime, zester_mtime):
                    print('!mtime', posix_search_path, zester_fid, posix_mtime, zester_mtime)
                if posix_mode != zester_mode: print('!mode', posix_search_path, zester_fid)
                if zester_type != 'd' and posix_size != zester_size:
                    print('!size', 'zester_type={0:s}'.format(zester_type), 'zester_size={0:d}'.format(zester_size),
                          'posix_size={0:d}'.format(posix_size), posix_search_path, zester_fid)

                if posix_uid != zester_uid: print('!uid', posix_search_path, zester_fid)
                if posix_gid != zester_gid: print('!gid', posix_search_path, zester_fid)
            except Exception:
                print("Zester File Not Found in Posix DB: ", posix_search_path)
        zester_curr_row = zester_cursor.fetchone()

def check_posix_to_zester(posix_db, zester_db):
    posix_cursor = posix_db.cursor()
    posix_query = "SELECT path, uid, gid, ctime, mtime, atime, mode, size FROM metadata" #  ORDER BY path"
    posix_count = 0
    posix_cursor.execute(posix_query)
    posix_curr_row = posix_cursor.fetchone()
    while posix_curr_row:
        posix_count = posix_count + 1
        (posix_path, posix_uid, posix_gid, posix_ctime, posix_mtime, posix_atime, posix_mode,
         posix_size) = posix_curr_row
        zester_search_path = posix_path.replace('/mnt/td', '', 1)
        if not zester_search_path.startswith('.nodehealth'):
            zester_cursor = zester_db.cursor()
            try:
                zester_fid = names.path_to_fid(zester_db, zester_search_path)
                zester_query = "SELECT uid, gid, ctime, mtime, atime, mode, size FROM [metadata] where fid = ?"
                zester_result = zester_cursor.execute(zester_query, [zester_fid])
                zester_curr_row = zester_result.fetchone()
                if zester_curr_row is None:
                    print("not found", zester_search_path)
                    print(zester_search_path, zester_fid)
                else:
                    (zester_uid, zester_gid, zester_ctime, zester_mtime, zester_atime, zester_mode,
                     zester_size) = zester_curr_row
                    if not check_time(posix_atime, zester_atime): print(
                        '!atime', zester_search_path, zester_fid, posix_atime, zester_atime)
                    if not check_time(posix_ctime, zester_ctime): print(
                        '!ctime', zester_search_path, zester_fid, posix_ctime, zester_ctime)
                    if not check_time(posix_mtime, zester_mtime): print(
                        '!mtime', zester_search_path, zester_fid, posix_mtime, zester_mtime)
                    if posix_mode != zester_mode:
                        print('!mode', zester_search_path, zester_fid, posix_mode, zester_mode)
                    if posix_size != zester_size:
                        if stat.S_ISREG(posix_mode):
                            print('!size_file', zester_search_path, zester_fid, 'p:', posix_size, 'z:', zester_size)
                        else:
                            pass
                            # print('!size_notfile', zester_search_path, zester_fid, 'p:', posix_size, 'z:', zester_size)
                    if posix_uid != zester_uid:
                        print('!uid', zester_search_path, zester_fid, posix_uid, zester_uid)
                    if posix_gid != zester_gid:
                        print('!gid', zester_search_path, zester_fid, posix_gid, zester_gid)
            except UnicodeEncodeError:
                print('unicode exception', zester_search_path)
            zester_cursor.close()
        posix_curr_row = posix_cursor.fetchone()

def doCompare(zester_db_fname, posix_db_fname):
    posix_db = sqlite3.connect(posix_db_fname)
    zester_db = sqlite3.connect(zester_db_fname)
    print ('check_posix_to_zester_______________')
    check_posix_to_zester(posix_db, zester_db)
    print ('check_zester_to_posix_______________')
    check_zester_to_posix(posix_db, zester_db)


if __name__ == '__main__':
    doCompare('metadata.db', 'stats.db')
    # doCompare('22000/metadata.db', '22000/stats.db')
    # doCompare('1000000/metadata.db', '1000000/stats.db')
