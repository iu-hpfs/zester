#!/usr/bin/env python2
# -*- coding: utf-8 -*-

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

from __future__ import print_function
import binascii
import fileinput
import sqlite3
import sys
import time

import fidinfo
import linkinfo
import lovinfo
import metadata
import names
import util


# todo: implement these

def fid2int(fid):
    (fidseq, fidoid, fidver) = fid.rsplit(':', 2)
    fidseq = int(fidseq, 16)
    fidoid = int(fidoid, 16)
    fidver = int(fidver, 16)
    return fidseq, fidoid, fidver


def int2fid(fidseq, fidoid, fidver=0x0):
    fid = hex(fidseq) + ':' + hex(fidoid) + ':' + hex(fidver)
    return fid


def get_fids_for_uid(conn, uid):
    #old_text_factory = conn.text_factory
    #conn.text_factory = str
    cur = conn.cursor()

    sql = "select fid from metadata where uid = ?"
    search_output_list = cur.execute(sql, [uid])
    fids = []
    for (fid,) in search_output_list:
        fids.append(fid)
        # Some debugging info here
        paths = names.fid_to_path(conn, fid)
        for path in paths:
            print('[{fid:s}] {path:s}'.format(fid=fid, path=path))
    #conn.text_factory = old_text_factory


def get_entries_for_path(path):
    # fid=path_to_fid()
    # recur into .. join on mode in metadata .. basically listdir
    raise NotImplementedError


def get_entries_for_uid_in_path(uid, path):
    entries = []
    for fid in get_fids_for_uid(uid):
        for path in names.fid_to_path(fid):
            entries.append(path)
    return entries


# zfs-db____

def open_zfsobj_db(zfsobj_db_fname):
    zfsobj_db = sqlite3.connect(zfsobj_db_fname)
    return zfsobj_db


'''
zfsobj   [zfsobj_id, uid, gid, ..., size, trusted_lov, trusted_link]
name     [fid, name, parent_fid, pk(name, parent_fid), index(fid)]
metadata [fid, uid, gid, ..., size]
'''


def setup_zfsobj_db(zfsobj_db_fname):
    zfsobj_db = open_zfsobj_db(zfsobj_db_fname)
    zfsobj_cur = zfsobj_db.cursor()
    zfsobj_cur.execute('drop index if exists zfsobj_trustedlov_index')
    zfsobj_cur.execute('drop table if exists zfsobj')
    zfsobj_cur.execute('''create table zfsobj (id integer primary key,
                        uid integer, gid integer, ctime integer, mtime integer, 
                        atime integer, mode integer, obj_type text, size integer, 
                        fid text, fidseq integer, fidoid integer, fidver integer, trusted_link text, trusted_lov text)
                        ''')
    zfsobj_cur.close()
    return zfsobj_db


def create_zfsobj_db_indices(zfsobj_db):
    zfsobj_cur = zfsobj_db.cursor()
    zfsobj_cur.execute('create index zfsobj_trustedlov_index on '
                       'zfsobj (trusted_lov)')
    zfsobj_cur.execute('create index zfsobj_fid_index on zfsobj (fidseq, fidoid, fidver)')
    zfsobj_db.commit()
    zfsobj_cur.close()


def save_zfs_obj(zfs_cur, obj_dict):
    obj_id = obj_dict['obj_id']
    if zfs_cur is not None:
        cmd = '''insert into [zfsobj] 
                 (id, uid, gid, ctime, mtime, atime, mode, obj_type, size,
                 fid, fidseq, fidoid, fidver, trusted_link, trusted_lov)
                 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

        fid = obj_dict.get('fid', None)
        (fidseq, fidoid, fidver) = fid2int(fid)

        zfs_cur.execute(cmd, (
            obj_id, obj_dict.get('uid', None), obj_dict.get('gid', None),
            obj_dict.get('ctime', None), obj_dict.get('mtime', None),
            obj_dict.get('atime', None), obj_dict.get('mode', None),
            obj_dict.get('obj_type', None), obj_dict.get('size', None),
            fid, fidseq, fidoid, fidver,
            obj_dict.get('trusted.link', None),
            obj_dict.get('trusted.lov', None)))


# parsing____

def parse_zdb(zdb_fname, zfsobj_db_fname):
    count = 0
    obj_id = None
    obj_type = None
    start = time.time()
    obj_dict = None
    dataset_name = None

    inputfile = fileinput.input([zdb_fname])

    zfsobj_db = setup_zfsobj_db(zfsobj_db_fname)
    zfsobj_cur = zfsobj_db.cursor()

    try:
        for line in inputfile:
            if line.startswith("Metaslabs:"):
                dataset_name = None  # todo: (old) parse and store?
            if line.startswith("Dirty time logs:"):
                # todo: (old) parse and store?
                dataset_name = None
            if line.startswith("Dataset mos [META]"):
                dataset_name = None
            if line.startswith("Dataset"):
                dataset_name = line.split(" ")[1]
                if '/' not in dataset_name:
                    dataset_name = None
            # if line.startswith("Dataset"):
            #     if dataset_name is not None:
            #         raise Exception('parseZdb',
            #                         'unexpected mulitple datasets in '
            #                         'ZDB dump')
                else:
                    pass
                    print("dataset_name: " + dataset_name)

            # Does this line starts a new object section?
            #
            # Work out whether the dump objects include the newer schema, with 'dnsize' or not,
            # and then do the right thing.
            # zfs 0.6.4
            zfs_obj_match_old = (line == '    Object  lvl   iblk   dblk  dsize  lsize   %full  type\n')
            # zfs 0.7.5
            zfs_obj_match_new = (line == '    Object  lvl   iblk   dblk  dsize  dnsize  lsize   %full  type\n')

            if zfs_obj_match_old:
                zfs_obj_match = zfs_obj_match_old
            else:
                zfs_obj_match = zfs_obj_match_new

            # If we see a new object line, save record and start parsing this object.
            if dataset_name and zfs_obj_match:
                if obj_dict is not None:
                    if obj_type == 'f' and obj_dict.get('fid') is not None:
                        save_zfs_obj(zfsobj_cur, obj_dict)
                        count = count + 1
                    elif obj_type == 'd' and obj_dict.get('trusted.link') is not None:
                        save_zfs_obj(zfsobj_cur, obj_dict)
                        count = count + 1
                if count % 15000 == 0:
                    if zfsobj_db is not None:
                        zfsobj_db.commit()
                        zfsobj_cur = zfsobj_db.cursor()
                        ts = time.time()
                        if ts > start:
                            util.show_timing(count, start, ts)
                data_line = inputfile.readline().rstrip()

                if zfs_obj_match_old:                       # without 'dnsize' in line, need to split 7 times
                    zfs_data = data_line.split(None, 7)
                    obj_id = int(zfs_data[0])
                    obj_type = zfs_data[7]
                elif zfs_obj_match_new:
                    zfs_data = data_line.split(None, 8)     # with 'dnsize' in line, need to split 8 times
                    obj_id = int(zfs_data[0])
                    obj_type = zfs_data[8]
                else:
                    obj_type = None
                    obj_id = None
                    print('ZDB input did not match expected object line format.')  # This better not occur!

                # Modify obj_type value to 'f' for 'ZFS plain file' and 'd' for 'ZFS directory', to compact storage.
                if obj_type == 'ZFS plain file':
                    obj_type = 'f'
                elif obj_type == 'ZFS directory':
                    obj_type = 'd'

                obj_dict = {'obj_id': obj_id, 'obj_type': obj_type}
            # Saw this in 0.7.5-based 'zdb -dddd' output on pool; need to skip these four lines
            elif 'Dnode slots:' in line:
                # Occurs at the end of the dataset info:
                # skip next 3 lines
                inputfile.readline()
                inputfile.readline()
                inputfile.readline()
            elif dataset_name and (obj_id is not None) and (
                    util.tabs_at_beginning(line) == 1):
                stripped = line[1:].rstrip('\n')
                if stripped.startswith(" "):
                    raise Exception("mixed-tab/space-indenting, "
                                    "further logic required")
                elif stripped.startswith("dnode flags: "):
                    pass
                elif stripped.startswith("dnode maxblkid: "):
                    pass
                elif stripped.startswith("path	"):
                    obj_dict['path'] = stripped.split("path	")[1]
                elif stripped.startswith("uid     "):
                    obj_dict['uid'] = int(stripped.split("uid     ")[1])
                elif stripped.startswith("gid     "):
                    obj_dict['gid'] = int(stripped.split("gid     ")[1])
                elif stripped.startswith("UNKNOWN OBJECT TYPE"):
                    # obj_dict['unkObjectType'] = True
                    pass
                elif stripped.startswith("atime	"):
                    obj_dict['atime'] = util.format_time_in_seconds(
                        stripped.split("atime	")[1])
                elif stripped.startswith("mtime	"):
                    obj_dict['mtime'] = util.format_time_in_seconds(
                        stripped.split("mtime	")[1])
                elif stripped.startswith("ctime	"):
                    obj_dict['ctime'] = util.format_time_in_seconds(
                        stripped.split("ctime	")[1])
                elif stripped.startswith("crtime	"):
                    obj_dict['crtime'] = util.format_time_in_seconds(
                        stripped.split("crtime	")[1])
                elif stripped.startswith("rdev	"):
                    pass
                elif stripped.startswith("SA xattrs: "):
                    if len(inputfile.readline().strip()) > 0:
                        raise Exception("Expected Blank Line pre xattrs")
                    line = inputfile.readline().strip()
                    while ' = ' in line:
                        pair = line.lstrip().split(' = ', 1)
                        name = pair[0]
                        if name == 'trusted.fid':
                            octal_fid = pair[1]
                            obj_dict['fid'] = str(fidinfo.decode_fid(octal_fid))
                        if name == 'trusted.link':
                            obj_dict['trusted.link'] = pair[1]
                        # Only try to decode trusted.lov for files. Turns out striped directories have
                        # a trusted.lov entry, which will break this code segment. They should have
                        # fids decoded with 'trusted.lma' below.
                        if name == 'trusted.lov' and obj_type == 'f':
                            trusted_lov = obj_dict['trusted.lov'] = pair[1]
                            # todo: eval decoding fid in later pass
                            try:
                                trusted_lov_hex = binascii.hexlify(
                                    str(fidinfo.decoder(trusted_lov)))

                                parsed_lov = lovinfo.parseLovInfo(trusted_lov_hex)
                                fid = hex(
                                    int(parsed_lov['lmm_seq'], 16)) + ':' + hex(
                                    int(parsed_lov['lmm_object_id'],
                                        16)) + ':0x0'
                                obj_dict['fid'] = fid
                            except:
                                print('')
                                print('trusted_lov decoding failed for this entry...')
                                print('    trusted_lov = ' + trusted_lov)
                                print('trusted_lov_hex = ' + trusted_lov_hex)
                                print('     parsed_lov = ', parsed_lov)
                                print('            fid = ' + fid)
                                print('       obj_type = ' + obj_type)
                                pass
                        # We need a fid for directories, which do not have a 'trusted.lov' EA
                        if name == 'trusted.lma' and obj_type == 'd':
                            #  trusted.lma is u32:u32:fid in little-endian
                            #  where fid is u64:u32:u32.
                            #  So trim the first 8 bytes to leave the fid
                            trusted_lma = pair[1]
                            fid = str(fidinfo.decode_fid(trusted_lma[32:]))
                            obj_dict['fid'] = fid

                        line = inputfile.readline().strip()
                    if "UNKNOWN OBJECT TYPE" in line:
                        pass
                elif stripped.startswith("gen	"):
                    pass
                elif stripped.startswith("mode	"):
                    obj_dict['mode'] = int(stripped.split("mode	")[1], 8)
                elif stripped.startswith("size	"):
                    obj_dict['size'] = int(stripped.split("size	")[1])
                elif stripped.startswith("parent	"):
                    pass
                elif stripped.startswith("links	"):
                    pass
                elif stripped.startswith("pflags	"):
                    pass
                elif stripped.startswith("Fat ZAP stats:"):
                    pass
                elif stripped.startswith("microzap: "):
                    pass
                else:
                    msg0 = "UNKNOWN 1-tab attribute: [dataset:{0}]" \
                           "[obj_id:{1}][{2}]"
                    raise Exception(msg0.format(dataset_name, obj_id, stripped))

        if obj_type == 'f' and obj_dict.get('fid') is not None:
            save_zfs_obj(zfsobj_cur, obj_dict)
        elif obj_type == 'd' and obj_dict.get('trusted.link') is not None:
            save_zfs_obj(zfsobj_cur, obj_dict)

    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
        raise

    create_zfsobj_db_indices(zfsobj_db)
    zfsobj_db.commit()
    zfsobj_db.close()


# persist-db

def lookup(ost_dbs0, ost_idx, fidseq, fidoid):
    ost_zfsobj_db = ost_dbs0[ost_idx]
    # fid.rsplit(':',1)[0] trims off the version, which is different for each
    # ost stripe of the FID with a 0x0 version.
    # partialfid = fid.rsplit(':', 1)[0] + ':%'

    zfsobj_cursor = ost_zfsobj_db.cursor()
    zfsobj_cursor.execute('''select size from zfsobj where fidseq = ? and fidoid = ?''',
                          [fidseq, fidoid])
    all_row = zfsobj_cursor.fetchall()
    zfsobj_cursor.close()

    if len(all_row) > 1:
        fid = int2fid(fidseq, fidoid, 0x0)
        print('[zester.lookup()]: More than one fid seq:oid match to [{0:s}] in ost index {1:d}.'.format(fid, ost_idx))

    size_of_stripes_on_ost = 0
    for zfsobj_row in all_row:
        (size,) = zfsobj_row
        size_of_stripes_on_ost = size_of_stripes_on_ost + size

    return size_of_stripes_on_ost


def get_total_size(ost_dbs0, parsed_lov):
    total_size = 0
    parsed_raw = parsed_lov['ost_index_objids']
    ost_index_objids = map(lambda tup: (int(tup[0]), int(tup[1])), parsed_raw)

    # Create a unique list of OST indices which contain stripes for this file.
    unique_ost_list = []
    for lov_ost_idx, lov_obj_idx in ost_index_objids:
        if lov_ost_idx not in unique_ost_list:
            unique_ost_list.append(lov_ost_idx)

    # This sort isn't necessary, but I am leaving it in for now, for any further debugging.
    unique_ost_list.sort()

    for lov_ost_idx in unique_ost_list:
        fidseq = int(parsed_lov['lmm_seq'], 16)
        fidoid = int(parsed_lov['lmm_object_id'], 16)

        stripe_size = lookup(ost_dbs0, lov_ost_idx, fidseq, fidoid)
        total_size = total_size + stripe_size

    return total_size


def commit_metadata_db(count, meta_cur, metadata_db, start):
    if count % 5000 == 0:
        metadata_db.commit()
        meta_cur = metadata_db.cursor()
        ts = time.time()
        util.show_timing(count, start, ts)
    return meta_cur


# conn = sqlite3.connect(":memory:")
# curs = conn.cursor()
# names.create_names_table(conn)
# names.insert_name(curs, dataset_id, fid, name, parent_fid)
# conn.commit()
# curs.close()
# names.fid_to_path(conn, srch_fid):

# linkinfo.parse_link_info(trusted_link_hex):
# -> [{'pfid': '0x200000400:0x2:0x0', 'filename': 'a'}, {'pfid': '0x240000401:0x2:0

def persist_names(name_db_fname, mdt_dbs0):
    name_db = sqlite3.connect(name_db_fname)
    names.setup_name_table(name_db)

    count = 0
    tstart = time.time()
    name_cur = name_db.cursor()

    # Insert an entry for the filesystem root, which in Lustre 2.x has a FID of [0x200000007:0x1:0x0].
    # See names.py for more info, and reference to Lustre source code.
    names.insert_name(name_cur, '0x200000007:0x1:0x0', 'ROOT', '')

    for mdt_dataset_id, mdt_dataset_db in mdt_dbs0.items():
        query = '''select fid, trusted_link from zfsobj'''
        mdt_cursor = mdt_dataset_db.cursor()
        mdt_cursor.execute(query)
        mdt_curr_row = mdt_cursor.fetchone()
        while mdt_curr_row is not None:
            (fid, trusted_link) = mdt_curr_row
            if trusted_link is not None:
                trusted_link_hex = binascii.hexlify(str(fidinfo.decoder(trusted_link)))
                try:
                    li_dicts = linkinfo.parse_link_info(trusted_link_hex)
                    for li_dict in li_dicts:
                        names.insert_name(name_cur, fid, li_dict['filename'],
                                          li_dict['pfid'])
                        count = count + 1
                except TypeError:
                    # todo: make sure this is normal
                    pass
                except ValueError:
                    # todo: make sure this is normal
                    pass
            mdt_curr_row = mdt_cursor.fetchone()
        mdt_cursor.close()
    print('total ' + str(count))
    print('comitting')
    name_db.commit()
    print('closing')
    name_cur.close()
    names.clean_remote_dirs(name_db)
    name_db.commit()
    name_db.close()
    print('done')

    dt = time.time() - tstart
    print('Serial names table creation in {0:f} seconds.'.format(dt))


def persist_object_writer(metadata_db_fname, resultq):
    import os
    pid = os.getpid()
    ppid = os.getppid()

    metadata_db = sqlite3.connect(metadata_db_fname)
    metadata.setup_metadata_db(metadata_db)

    meta_cur = metadata_db.cursor()

    while True:
        result = resultq.get()

        if result[0] == 'DONE':
            print("persist_object_writer with pid|ppid = {0:d}|{1:d} saw 'DONE'. Exiting.".format(pid, ppid))
            print('committing metadata db')
            metadata_db.commit()
            print('closing metadata db')
            meta_cur.close()
            print('metadata db cursor closed')
            metadata_db.close()
            print('metadata db closed. Exiting')
            break
        else:
            (fid, uid, gid, ctime, mtime, atime, mode, size, obj_type) = result
            metadata.save_metadata_obj(meta_cur, fid, uid, gid, ctime, mtime, atime, mode, size, obj_type)


def persist_object_worker(ost_db_fnames, workq, resultq):
    import os
    pid = os.getpid()
    ppid = os.getppid()

    ost_dbs0 = {}
    for ikey in ost_db_fnames.keys():
        ost_dbs0[ikey] = sqlite3.connect(ost_db_fnames[ikey])

    while True:
        work = workq.get()

        if work[0] == 'DONE':
            print("persist_object_worker with pid|ppid = {0:d}|{1:d} saw 'DONE'. Exiting.".format(pid, ppid))
            for ikey in ost_dbs0.keys():
                ost_dbs0[ikey].close()
            break
        else:
            (id0, uid, gid, ctime, mtime, atime, mode, obj_type, size, fid, trusted_link, trusted_lov) = work
            if obj_type == 'f':
                if trusted_lov is not None:
                    parsed_lov = lovinfo.parseLovInfo(binascii.hexlify(str(fidinfo.decoder(trusted_lov))))
                    size = get_total_size(ost_dbs0, parsed_lov)
                else:
                    # Flag any cases where no trusted_lov exists from which to calculate file size.
                    size = -1
            elif obj_type == 'd':
                size = 0

            result = (fid, uid, gid, ctime, mtime, atime, mode, size, obj_type)
            resultq.put(result)


def persist_objects(metadata_db_fname, mdt_dbs0, ost_db_fnames):
    import multiprocessing as mp
    import sys

    nworkers = 6

    workq = mp.Queue()
    resultq = mp.Queue()

    print('Starting one metadata db writer process.')
    writerp = mp.Process(target=persist_object_writer, args=(metadata_db_fname, resultq))

    print('Starting {0:d} parallel worker processes.'.format(nworkers))
    workerp_list = []
    for i in range(nworkers):
        workerp_list.append(mp.Process(target=persist_object_worker, args=(ost_db_fnames, workq, resultq)))

    try:
        writerp.start()
        for p in workerp_list:
            p.start()

        # Now, loop over the contents of each MDT db, and delegate work to parallel workers.

        count = 0
        tstart = time.time()
        for mdt_dataset_id, mdt_dataset_db in mdt_dbs0.items():
            query = '''select id, uid, gid, ctime, mtime, atime, mode, obj_type,
                       size, fid, trusted_link, trusted_lov from zfsobj'''
            mdt_cursor = mdt_dataset_db.cursor()
            mdt_cursor.execute(query)
            mdt_curr_row = mdt_cursor.fetchone()
            while mdt_curr_row is not None:
                workq.put(mdt_curr_row)
                count = count + 1

                if count % 10000 == 0:
                    print('Persisted {0:08d} records.'.format(count))

                # Check to see if it's time to force a commit, and if so, do it.
                # meta_cur = commit_metadata_db(count, meta_cur, metadata_db, start)

                # Grab the next row.
                mdt_curr_row = mdt_cursor.fetchone()
            mdt_cursor.close()
        print('total ' + str(count))

        # Tell worker processes they can exit.
        print('Telling worker processes that work is DONE.')
        for i in range(nworkers):
            workq.put(('DONE',))

        # Wait for worker processes to exit.
        for p in workerp_list:
            if p.is_alive():
                p.join()
            else:
                print('persist_objects(): Worker process with name {0:s} was gone. No join needed.'.format(p.name))

        # Tell writer process it can exit.
        print('Telling writer process that work is DONE.')
        resultq.put(('DONE',))

        # Wait for the writer process to finish writing, committing,
        # and closing metadata db.
        if writerp.is_alive():
            writerp.join()
        else:
            print('persist_objects(): Writer process with name {0:s} was gone. No join needed.'.format(writerp.name))

        print('done')
    except KeyboardInterrupt:
        print('persist_objects(): Interrupted by keyboard. Terminating child processes...')
        for p in workerp_list:
            p.terminate()
        writerp.terminate()
        print('persist_objects(): Child processes terminated. Exiting.')
        sys.exit(1)
    dt = time.time() - tstart
    print('Parallel metadata table creation in {0:f} seconds.'.format(dt))


def persist(metadata_db_fname, mdt_db_fnames, ost_db_fnames):
    print('persisting objects')
    mdt_dbs0 = {}
    for ikey in mdt_db_fnames.keys():
        mdt_dbs0[ikey] = sqlite3.connect(mdt_db_fnames[ikey])

    persist_objects(metadata_db_fname, mdt_dbs0, ost_db_fnames)

    print('persisting names')
    name_db_fname = metadata_db_fname
    persist_names(name_db_fname, mdt_dbs0)

    for ikey in mdt_dbs0:
        mdt_dbs0[ikey].close()


def parse(file_paths):
    import multiprocessing as mp
    import sys

    # mdt_dbs0 = {}
    # ost_dbs0 = {}
    mdt_db_fnames = {}
    ost_db_fnames = {}

    proc_list = []
    for zdb_fname in file_paths:
        lustre_type, rest = zdb_fname.split('_', 1)
        pair = rest.split('.', 1)
        id0 = int(pair[0])
        dump_type = pair[1]
        if dump_type != "zdb":
            raise Exception("only zdb dumps currently supported")

        print('processing file: ' + zdb_fname)
        zfsobj_db_fname = lustre_type + '_' + str(id0) + '.db'

#    for zfsobj_db_fname in zfsobj_db_fnames:
        # parse_zdb(zdb_fname, zfsobj_db_fname)

        proc_list.append(mp.Process(target=parse_zdb, args=(zdb_fname, zfsobj_db_fname)))

    # CPU time used by the parent process will not reflect the time spent
    # waiting for parallel processes to finish. Use time.time().
    tstart = time.time()
    try:
        for p in proc_list:
            p.start()
        for p in proc_list:
            p.join()
    except KeyboardInterrupt:
        for p in proc_list:
            print('persist_objects(): Interrupted by keyboard. Terminating child processes...')
            p.terminate()
            print('persist_objects(): Child processes terminated. Exiting.')
            sys.exit(1)
    dt = time.time() - tstart

    print('Parallel ZDB consumption in {0:f} seconds.'.format(dt))

    # Need to open DBs to hand off DB handles to calling function (i.e. main())

    # I would like to separate the opening of sqlite db files in this routine. So, this is temporary
    # until the parallel zdb consumption part is done. Just repeating what happens above, to recreate
    # the filenames for sqlite db files before opening them.
    for zdb_fname in file_paths:
        lustre_type, rest = zdb_fname.split('_', 1)
        pair = rest.split('.', 1)
        id0 = int(pair[0])

        zfsobj_db_fname = lustre_type + '_' + str(id0) + '.db'
        # zfsobj_db = sqlite3.connect(zfsobj_db_fname)
        if lustre_type == 'mdt':
            # mdt_dbs0[id0] = zfsobj_db
            mdt_db_fnames[id0] = zfsobj_db_fname
        elif lustre_type == 'ost':
            # ost_dbs0[id0] = zfsobj_db
            ost_db_fnames[id0] = zfsobj_db_fname
        else:
            raise Exception("must be ost or mdt dump")
    return mdt_db_fnames, ost_db_fnames


# main____


msg = '''Usage: zester [OPTION]... mdt_<mdtidx>.zdb ... ost_<ostidx>.zdb ...
Parse MDT and ZDB dumps into a SQLite representation then assemble into a
queryable metadata.db SQLite DB file

Options:
 --parse         only parse ZDB dumps into SQLite DB, do not assemble
'''


def main():
    if len(sys.argv) < 2:
        print(msg)
        sys.exit(1)
    if sys.argv[1] == '--parse':
        parse(sys.argv[2:])
    else:
        mdt_db_fnames, ost_db_fnames = parse(sys.argv[1:])
        persist('metadata.db', mdt_db_fnames, ost_db_fnames)


if __name__ == '__main__':
    main()

    # import sqlite3
    # import names
    # db = sqlite3.connect('metadata.db')
    #
    # for path in names.fid_to_path(db, '0x200014b01:0x1:0x0'):
    #     print(path)
    #
    # for path in names.fid_to_path(db, '0x200014b01:0x2:0x0'):
    #     print(path)
    #
    # print(names.path_to_fid(db, 'slavin/unicode-test'))
    # print(names.path_to_fid(db, u'slavin/unicode-test/Šħâŵƞ'))
    #
    # db.close()
