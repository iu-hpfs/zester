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

def get_fids_for_uid(uid):
    raise NotImplementedError


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

def open_zfsobj_db(zfsobj_db_fame):
    zfsobj_db = sqlite3.connect(zfsobj_db_fame)
    zfsobj_db.text_factory = str
    return zfsobj_db


'''
zfsobj   [zfsobj_id, uid, gid, ..., size, trusted_lov, trusted_link]
name     [fid, name, parent_fid, pk(name, parent_fid), index(fid)]
metadata [fid, uid, gid, ..., size]
'''


def setup_zfsobj_db(zfsobj_db_fame):
    zfsobj_db = open_zfsobj_db(zfsobj_db_fame)
    zfsobj_cur = zfsobj_db.cursor()
    zfsobj_cur.execute('drop index if exists zfsobj_trustedlov_index')
    zfsobj_cur.execute('DROP TABLE IF EXISTS zfsobj')
    zfsobj_cur.execute('''CREATE TABLE zfsobj (id INTEGER PRIMARY KEY,
                        uid INTEGER, gid INTEGER, ctime INTEGER, mtime INTEGER, 
                        atime INTEGER, mode INTEGER, obj_type TEXT, size INTEGER, 
                        fid TEXT, trusted_link TEXT, trusted_lov TEXT)
                        ''')
    zfsobj_cur.close()
    return zfsobj_db


def save_zfs_obj(zfs_cur, obj_dict, dataset_dicts):
    id0 = obj_dict['id']
    obj_id = obj_dict['obj_id']
    if dataset_dicts is not None:
        dataset_dicts[id0][obj_id] = obj_dict
    if zfs_cur is not None:
        cmd = '''INSERT INTO [zfsobj] 
                 (id, uid, gid, ctime, mtime, atime, mode, obj_type, size,
                 fid, trusted_link, trusted_lov)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        if True:  # 'trusted.fid' in obj_dict or 'trusted.lov' in obj_dict:
            zfs_cur.execute(cmd, (
                obj_id, obj_dict.get('uid', None), obj_dict.get('gid', None),
                obj_dict.get('ctime', None), obj_dict.get('mtime', None),
                obj_dict.get('atime', None), obj_dict.get('mode', None),
                obj_dict.get('obj_type', None), obj_dict.get('size', None),
                obj_dict.get('fid', None), obj_dict.get('trusted.link', None),
                obj_dict.get('trusted.lov', None)))


# parsing____

def parse_zdb(id0, inputfile, zfsobj_db=None, dataset_dicts=None):
    count = 0
    obj_id = None
    start = time.clock()
    obj_dict = None
    dataset_name = None
    in_fat_zap = False
    if zfsobj_db is not None:
        zfsobj_cur = zfsobj_db.cursor()
    else:
        zfsobj_cur = None
    try:
        for line in inputfile:
            if line.startswith("Metaslabs:"):
                dataset_name = None  # todo: (old) parse and store?
            if line.startswith("Dirty time logs:"):
                # todo: (old) parse and store?
                dataset_name = None
            if line.startswith("Dataset mos [META]"):
                datasetName = None
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
                    print("dataset_name: " + dataset_name + " id: " + str(id0))
                if dataset_dicts is not None:
                    dataset_dicts[id0] = {}

            # Does this line starts a new object section?
            #
            # Work out whether the dump objects include the newer schema, with 'dnsize' or not,
            # and then do the right thing.
            #zfs 0.6.4
            zfs_obj_match_old = (line == '    Object  lvl   iblk   dblk  dsize  lsize   %full  type\n')
            #zfs 0.7.5
            zfs_obj_match_new = (line == '    Object  lvl   iblk   dblk  dsize  dnsize  lsize   %full  type\n')

            if zfs_obj_match_old:
                zfs_obj_match = zfs_obj_match_old
            else:
                zfs_obj_match = zfs_obj_match_new

            # If we see a new object line, clear and start parsing this object.
            if dataset_name and zfs_obj_match:
                if obj_dict is not None:
                    save_zfs_obj(zfsobj_cur, obj_dict, dataset_dicts)
                count = count + 1
                if count % 15000 == 0:
                    if zfsobj_db is not None:
                        zfsobj_db.commit()
                        zfsobj_cur = zfsobj_db.cursor()
                    ts = time.clock()
                    util.show_timing(count, start, ts)
                in_fat_zap = False
                data_line = inputfile.readline().rstrip()

                if zfs_obj_match_old:                       # without 'dnsize' in line, need to split 7 times
                    zfs_data = data_line.split(None, 7)
                    obj_id = int(zfs_data[0])
                    obj_type = zfs_data[7]
                elif zfs_obj_match_new:
                    zfs_data = data_line.split(None, 8)     # with 'dnsize' in line, need to split 8 times
                    obj_id = int(zfs_data[0])
                    obj_type = zfs_data[8]

                obj_dict = {'id': id0, 'obj_id': obj_id, 'obj_type': obj_type}
            elif in_fat_zap:
                # if (tabs_at_beginning(line) == 2) and (line[2].isdigit()) and (
                #         '=' in line) and ('type: ' in line):
                #     chopped = line.split('(type: ')
                #     pair = chopped[0].strip().split(' = ')
                #     type0 = chopped[1].rstrip().rstrip(')')
                #     if type0 == 'Regular File':
                #         name = int(pair[0])
                #         idx = int(pair[1])
                #         if 'fatZap' not in obj_dict:
                #             obj_dict['fatZap'] = {}
                #         obj_dict['fatZap'][name] = {'target': idx,
                #                                     'type': type0}
                #     else:
                #         pass
                #  print('Not saving info for type {0:s}.'.format(type))
                pass
                #                        print('Not saving info for type {0:s}.'.format(type))
            # Saw this in 0.7.5-based 'zdb -dddd' output on pool; need to skip these four lines
            elif 'Dnode slots:' in line:
                print( "Saw Dnode slots line" )
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
                        if name == 'trusted.lov':
                            trusted_lov = obj_dict['trusted.lov'] = pair[1]
                            # todo: eval decoding fid in later pass
                            try:
                                tmphexlov = binascii.hexlify(
                                    str(fidinfo.decoder(trusted_lov)))
                                parsed = lovinfo.parseLovInfo(tmphexlov)
                                obj_dict['objects'] = str(
                                    parsed['ost_index_objids'])
                                parsed_lov = lovinfo.parseLovInfo(
                                    binascii.hexlify(
                                        str(fidinfo.decoder(trusted_lov))))
                                fid = hex(
                                    int(parsed_lov['lmm_seq'], 16)) + ':' + hex(
                                    int(parsed_lov['lmm_object_id'],
                                        16)) + ':0x0'
                                obj_dict['fid'] = fid
                            except:
                                if 'ZFS directory' not in obj_dict['obj_type']:
                                    raise
                                pass
                        if name == 'trusted.lma':
                            # trusted.lma is u32:u32:fid in little-endian
                            #  where fid is u64:u32:u32.
                            #  So trim the first 8 bytes to leave the fid
                            #  fid2 = str(fidinfo.decode_fid(trusted_lma[32:]))
                            trusted_lma = pair[1]

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
                    in_fat_zap = True
                elif stripped.startswith("microzap: "):
                    pass
                else:
                    msg0 = "UNKNOWN 1-tab attribute: [dataset:{0}]" \
                           "[obj_id:{1}][{2}]"
                    raise Exception(msg0.format(dataset_name, obj_id, stripped))
        if obj_dict is not None:
            save_zfs_obj(zfsobj_cur, obj_dict, dataset_dicts)
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
        raise
    if zfsobj_db is not None:
        zfsobj_db.commit()
    return id0


# persist-db____

# def lookup(ost_dbs, ostIdx, objId):
#    ost_zfsobj_db = ost_dbs[ostIdx]
#    query = 'select id, from_id, to_id, zfs_type from fatzap where ' \
#            'from_id=' + str(objId) + ' and zfs_type="Regular File"'
# todo: switch to new methodology
def lookup(ost_dbs0, ost_idx, fid):
    ost_zfsobj_db = ost_dbs0[ost_idx]
    # fid.rsplit(':',1)[0] trims off the version, which is different for each
    # ost stripe of the FID with a 0x0 version.

    partialfid = fid.rsplit(':', 1)[0] + ':%'
    # Note: It's important to 
    #    query = 'select id, from_id, to_id, zfs_type from fatzap where ' \
    #            'fid like "' + partialfid + '" and zfs_type="Regular File"'
    #    fatzap_cursor = ost_zfsobj_db.cursor()
    #    fatzap_cursor.execute(query)
    #    all_fatzap = fatzap_cursor.fetchall()
    #    fatzap_cursor.close()

    # Ask Ken about this. [2018-05-09, SDS]
    #    if len(all_fatzap) > 1:
    #        print('objId, bam zap tot: ' + str(objId) + ', ' + str(len(all_fatzap)))

    #    for fatzap_row in all_fatzap:
    #        (id, from_id, to_id, zfs_type) = fatzap_row
    zfsobj_cursor = ost_zfsobj_db.cursor()
    zfsobj_cursor.execute('''SELECT id, uid, gid, ctime, mtime, atime, mode, size,
        fid, trusted_link, trusted_lov FROM zfsobj where fid like "''' + partialfid + '"')
    all_row = zfsobj_cursor.fetchall()
    zfsobj_cursor.close()

    if len(all_row) > 1:
        print(
        'More than one partial fid match to [', partialfid, '] in ost index',
        ost_idx)

    size_of_stripes_on_ost = 0
    for zfsobj_row in all_row:
        (id0, uid, gid, ctime, mtime, atime, mode, size, fid, trusted_link,
         trusted_lov) = zfsobj_row
        size_of_stripes_on_ost = size_of_stripes_on_ost + size

    return size_of_stripes_on_ost


def get_total_size(ost_dbs0, parsed_lov):
    total_size = 0
    parsed_raw = parsed_lov['ost_index_objids']
    ost_index_objids = map(lambda tup: (int(tup[0]), int(tup[1])), parsed_raw)
    # This is where we can make client calls to lookup sizes in parallel...
    for lovOstIdx, lovObjIdx in ost_index_objids:
        #                total_size = total_size + lookup(ost_dbs, lovOstIdx,
        #                                               lovObjIdx)

        fid = hex(int(parsed_lov['lmm_seq'], 16)) + ':' + hex(
            int(parsed_lov['lmm_object_id'], 16)) + ':0x0'
        total_size = total_size + lookup(ost_dbs0, lovOstIdx, fid)
    return total_size


def commit_meta_db(count, meta_cur, meta_db, start):
    if count % 5000 == 0:
        meta_db.commit()
        meta_cur = meta_db.cursor()
        ts = time.clock()
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

def persist_names(name_db, mdt_dbs0):
    count = 0
    start = time.clock()
    name_cur = name_db.cursor()
    for mdt_dataset_id, mdt_dataset_db in mdt_dbs0.items():
        query = '''SELECT fid, trusted_link from zfsobj'''
        mdt_cursor = mdt_dataset_db.cursor()
        mdt_cursor.execute(query)
        mdt_curr_row = mdt_cursor.fetchone()
        while mdt_curr_row is not None:
            (fid, trusted_link) = mdt_curr_row
            if trusted_link is not None:
                try:
                    li_dicts = linkinfo.parse_link_info(trusted_link)
                    for li_dict in li_dicts:
                        names.insert_name(name_cur, fid, li_dict['filename'],
                                          li_dict['pfid'])
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
        print('done')


def persist_objects(meta_db, mdt_dbs0, ost_dbs0):
    count = 0
    start = time.clock()
    meta_cur = meta_db.cursor()
    for mdt_dataset_id, mdt_dataset_db in mdt_dbs0.items():
        query = '''SELECT id, uid, gid, ctime, mtime, atime, mode, obj_type,
                  size, fid, trusted_link, trusted_lov FROM zfsobj'''
        mdt_cursor = mdt_dataset_db.cursor()
        mdt_cursor.execute(query)
        mdt_curr_row = mdt_cursor.fetchone()
        while mdt_curr_row is not None:
            (id0, uid, gid, ctime, mtime, atime, mode, obj_type, size, fid,
             trusted_link, trusted_lov) = mdt_curr_row
            # todo: had to add obj_type back to zfsobj, re-evaluate later
            if trusted_lov and obj_type == 'ZFS plain file' is not None:
                count = count + 1
                meta_cur = commit_meta_db(count, meta_cur, meta_db, start)

                parsed_lov = lovinfo.parseLovInfo(
                    binascii.hexlify(str(fidinfo.decoder(trusted_lov))))
                size = get_total_size(ost_dbs0, parsed_lov)

                metadata.save_metadata_obj(meta_cur, fid, uid, gid, ctime,
                                           mtime, atime, mode, size)
            mdt_curr_row = mdt_cursor.fetchone()
        mdt_cursor.close()
    print('total ' + str(count))
    print('comitting')
    meta_db.commit()
    print('closing')
    meta_cur.close()
    print('done')


def persist(metadata_db_fname, name_db_fname, mdt_dbs0, ost_dbs0):
    print('persisting objects')
    meta_db = sqlite3.connect(metadata_db_fname)
    meta_db.text_factory = str
    metadata.setup_metadata_db(meta_db)
    persist_objects(meta_db, mdt_dbs0, ost_dbs0)

    print('persisting names')
    name_db = sqlite3.connect(name_db_fname)
    name_db.text_factory = str
    names.setup_name_table(name_db)
    persist_names(meta_db, mdt_dbs0)

    print('bulding metadata indexes')
    meta_cur = meta_db.cursor()
    meta_db.commit()
    meta_cur.close()
    meta_db.close()


def parse(file_paths):
    mdt_dbs0 = {}
    ost_dbs0 = {}
    for name in file_paths:
        start = time.clock()
        lustre_type, rest = name.split("_")
        pair = rest.split(".")
        id0 = int(pair[0])
        dump_type = pair[1]
        print('processing file: ' + name)
        zfsobj_db_fname = lustre_type + '_' + str(id0) + '.db'
        zfsobj_db = setup_zfsobj_db(zfsobj_db_fname)
        if dump_type != "zdb":
            raise Exception("only zdb dumps currently supported")
        count = parse_zdb(id0, fileinput.input([name]), zfsobj_db)
        zfsobj_cur = zfsobj_db.cursor()
        zfsobj_cur.execute('create index zfsobj_trustedlov_index on '
                           'zfsobj (trusted_lov)')
        #        zfsobj_cur.execute('create index fatzap_from_id_index on fatzap '
        #                           '(from_id)')
        zfsobj_db.commit()
        ts = time.clock()
        util.show_timing(count, start, ts)
        if lustre_type == 'mdt':
            mdt_dbs0[id0] = zfsobj_db
        elif lustre_type == 'ost':
            ost_dbs0[id0] = zfsobj_db
        else:
            raise Exception("must be ost or mdt dump")
    return mdt_dbs0, ost_dbs0


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
        mdt_dbs0, ost_dbs0 = parse(sys.argv[1:])
        persist('metadata.db', 'name.db', mdt_dbs0, ost_dbs0)


if __name__ == '__main__':
    main()
