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
import cPickle as pickle
import fileinput
import sqlite3
import sys
import time
from datetime import datetime
from time import mktime

import fidinfo
import lovinfo
import metadata

# zfs-db____

def open_zfsobj_db(zfsobjDbFame):
    zfsobj_db = sqlite3.connect(zfsobjDbFame)
    zfsobj_db.text_factory = str
    return zfsobj_db

def setup_zfsobj_db(zfsobjDbFame):
    zfsobj_db = open_zfsobj_db(zfsobjDbFame)
    zfsobj_cur = zfsobj_db.cursor()
    zfsobj_cur.execute('drop index if exists zfsobj_trustedlov_index')
#    zfsobj_cur.execute('drop index if exists fatzap_from_id_index')
    zfsobj_cur.execute('DROP TABLE IF EXISTS zfsobj')
    zfsobj_cur.execute(
        'CREATE TABLE zfsobj (id INTEGER PRIMARY KEY, path TEXT,'
        ' uid INTEGER, gid INTEGER, ctime INTEGER, mtime INTEGER, '
        ' atime INTEGER, mode INTEGER, objType CHAR(1), ' +
        ' size INTEGER, trustedFid TEXT, trustedLov TEXT,'
        ' objects TEXT, fid TEXT)')
#    zfsobj_cur.execute('DROP TABLE IF EXISTS fatzap')
#    zfsobj_cur.execute(
#        'CREATE TABLE fatzap (id INTEGER, fid TEXT, from_id INTEGER,'
#        ' to_id INTEGER, zfs_type type TEXT, PRIMARY KEY (id, from_id, to_id))')
    zfsobj_cur.close()
    return zfsobj_db

def save_zfs_obj(zfs_cur, obj_dict, dataset_dicts):
    id = obj_dict['id']
    objID = obj_dict['objID']
    if dataset_dicts is not None:
        dataset_dicts[id][objID] = obj_dict
    if zfs_cur is not None:
        cmd = 'INSERT INTO [zfsobj] ' + \
              '(id, path, uid, gid, ctime, mtime, atime, mode, objType, size,' \
              ' trustedFid, trustedLov, objects, fid)' + \
              ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        if True:  # 'trusted.fid' in obj_dict or 'trusted.lov' in obj_dict:
            zfs_cur.execute(cmd, (objID,
                                  obj_dict.get('path', None),
                                  obj_dict.get('uid', None),
                                  obj_dict.get('gid', None),
                                  obj_dict.get('ctime', None),
                                  obj_dict.get('mtime', None),
                                  obj_dict.get('atime', None),
                                  obj_dict.get('mode', None),
                                  obj_dict.get('objType', None),
                                  obj_dict.get('size', None),
                                  obj_dict.get('trusted.fid', None),
                                  obj_dict.get('trusted.lov', None),
                                  obj_dict.get('objects', None),
                                  obj_dict.get('fid', None),
                                  ))
        fid = obj_dict.get('fid', None)
#        cmd = 'INSERT INTO [fatzap] ' \
#              '(id, fid, from_id, to_id, zfs_type)' + \
#              ' VALUES (?, ?, ?, ?, ?)'

#        if 'fatZap' in obj_dict:
#            for key, val in obj_dict['fatZap'].items():
#                zfs_cur.execute(cmd, (objID, fid, key, val['target'], val['type']))

# utility code

def showTiming(count, start, ts):
    tot_per_sec = int(count / (ts - start))
    secs_in_day = 86400
    tot_per_day = secs_in_day * tot_per_sec
    if tot_per_day > 0:
        days_per_billion = 1000000000 / tot_per_day
        print(str(count) + " total (" + str(tot_per_sec) + "/s)")
        # (" + str(days_per_billion) + " days)")
    sys.stdout.flush()

def serialize(filename, obj):
    pickle_file = open(filename, 'wb')
    pickle.dump(obj, pickle_file)
    pickle_file.close()

def formatTimeInSeconds(datetime_object):
    return int(mktime(datetime.strptime(datetime_object,
                                        '%a %b %d %H:%M:%S %Y').timetuple()))

def deserialize(filename):
    pickle_file = open(filename, 'rb')
    obj = pickle.load(pickle_file)
    pickle_file.close()
    return obj

def tabsAtBeginning(line):
    return len(line) - len(line.lstrip('\t'))

def writeObject(filename, obj):
    input_filename = open(filename, "w")
    input_filename.write(obj)
    input_filename.close()

# parsing____

def parseZdb(id, inputfile, zfsobj_db=None, datasetDicts=None):
    count = 0
    objID = None
    start = time.clock()
    objDict = None
    datasetName = None
    inFatZap = False
    if zfsobj_db is not None:
        zfsobj_cur = zfsobj_db.cursor()
    else:
        zfsobj_cur = None
    try:
        for line in inputfile:
            if line.startswith("Metaslabs:"):
                datasetName = None
                # todo: parse and store?
            if line.startswith("Dirty time logs:"):
                # todo: parse and store?
                datasetName = None
            if line.startswith("Dataset"):
                if datasetName is not None:
                    raise Exception('parseZdb',
                                    'unexpected mulitple datasets in '
                                    'ZDB dump')
                datasetName = line.split(" ")[1]
                print("datasetName: " + datasetName + " id: " + str(id))
                if datasetDicts is not None:
                    datasetDicts[id] = {}
            # does this line starts a new object section?
            zfsObjMatch = (line == '    Object  lvl   iblk   dblk  dsize  lsize   %full  type\n')
            # if we see a new object line, clear and start parsing this object.
            if datasetName and zfsObjMatch:
                if objDict is not None:
                    save_zfs_obj(zfsobj_cur, objDict, datasetDicts)
                count = count + 1
                if (count % 15000 == 0):
                    if zfsobj_db is not None:
                        zfsobj_db.commit()
                        zfsobj_cur = zfsobj_db.cursor()
                    ts = time.clock()
                    showTiming(count, start, ts)
                inFatZap = False
                dataLine = inputfile.readline().rstrip()
                zfsData = dataLine.split(None, 7)
                objID = int(zfsData[0])
                objType = zfsData[7]
                objDict = {'id': id, 'objID': objID, 'objType': objType}
            elif inFatZap:
                if (tabsAtBeginning(line) == 2) and (line[2].isdigit()) and ('=' in line) and ('type: ' in line):
                    chopped = line.split('(type: ')
                    pair = chopped[0].strip().split(' = ')
                    type = chopped[1].rstrip().rstrip(')')
                    if (type == 'Regular File'):
                        try:
                            name = int(pair[0])
                            idx = int(pair[1])
                            if 'fatZap' not in objDict:
                                objDict['fatZap'] = {}
                            objDict['fatZap'][name] = {'target': idx,
                                                       'type': type}
                        except:
                            pass
                    else:
                        pass
#                        print('Not saving info for type {0:s}.'.format(type))
            elif datasetName and (objID is not None) and \
                    (tabsAtBeginning(line) == 1):
                stripped = line[1:].rstrip('\n')
                if stripped.startswith(" "):
                    raise Exception("mixed-tab/space-indenting, "
                                    "further logic required")
                elif stripped.startswith("dnode flags: "):
                    pass
                elif stripped.startswith("dnode maxblkid: "):
                    pass
                elif stripped.startswith("path	"):
                    objDict['path'] = stripped.split("path	")[1]
                elif stripped.startswith("uid     "):
                    objDict['uid'] = int(stripped.split("uid     ")[1])
                elif stripped.startswith("gid     "):
                    objDict['gid'] = int(stripped.split("gid     ")[1])
                elif stripped.startswith("UNKNOWN OBJECT TYPE"):
                    # objDict['unkObjectType'] = True
                    pass
                elif stripped.startswith("atime	"):
                    objDict['atime'] = formatTimeInSeconds(
                        stripped.split("atime	")[1])
                elif stripped.startswith("mtime	"):
                    objDict['mtime'] = formatTimeInSeconds(
                        stripped.split("mtime	")[1])
                elif stripped.startswith("ctime	"):
                    objDict['ctime'] = formatTimeInSeconds(
                        stripped.split("ctime	")[1])
                elif stripped.startswith("crtime	"):
                    objDict['crtime'] = formatTimeInSeconds(
                        stripped.split("crtime	")[1])
                elif stripped.startswith("rdev	"):
                    pass
                elif stripped.startswith("SA xattrs: "):
                    if (len(inputfile.readline().strip()) > 0):
                        raise Exception("Expected Blank Line pre xattrs")
                    line = inputfile.readline().strip()
                    while (' = ' in line):
                        pair = line.lstrip().split(' = ',1)
                        name = pair[0]
                        if name == 'trusted.fid':
                            octal_fid = pair[1]
                            try:
                                objDict['trusted.fid'] = octal_fid
                                objDict['fid'] = str(fidinfo.decode_fid(octal_fid))
                            except:
                                raise
                        if name == 'trusted.lov':
                            trustedLov = objDict['trusted.lov'] = pair[1]
                            try:
                                tmphexlov=binascii.hexlify(str(fidinfo.decoder(trustedLov)))
                                parsed = lovinfo.parseLovInfo(tmphexlov)
                                objDict['objects'] = str(parsed['ost_index_objids'])
                                parsed_lov = lovinfo.parseLovInfo(binascii.hexlify(str(fidinfo.decoder(trustedLov))))
                                fid = hex(int(parsed_lov['lmm_seq'],16)) + ':' + hex(int(parsed_lov['lmm_object_id'],16)) + ':0x0'
                                objDict['fid'] = fid
#                                if objDict['path'].endswith('tmp749380'):
#                                    print( objDict );
                            except:
                                if 'ZFS directory' not in objDict['objType']:
                                    raise
                                pass
                        if name == 'trusted.lma':
                            trustedLma = pair[1]
                            # trusted.lma is u32:u32:fid in little-endian
                            # where fid is u64:u32:u32.
                            # So trim the first 8 bytes to leave the fid
                            fid2 = str(fidinfo.decode_fid(trustedLma[32:]))

                        line = inputfile.readline().strip()
                    if ("UNKNOWN OBJECT TYPE" in line):
                        pass
                elif stripped.startswith("gen	"):
                    pass
                elif stripped.startswith("mode	"):
                    objDict['mode'] = int(stripped.split("mode	")[1], 8)
                elif stripped.startswith("size	"):
                    objDict['size'] = int(stripped.split("size	")[1])
                elif stripped.startswith("parent	"):
                    pass
                elif stripped.startswith("links	"):
                    pass
                elif stripped.startswith("pflags	"):
                    pass
                elif stripped.startswith("Fat ZAP stats:"):
                    inFatZap = True
                elif stripped.startswith("microzap: "):
                    pass
                else:
                    msg = "UNKNOWN 1-tab attribute: [dataset:{0}]" \
                          "[objID:{1}][{2}]"
                    raise Exception(
                        msg.format(datasetName, objID, stripped))
        if objDict is not None:
            save_zfs_obj(zfsobj_cur, objDict, datasetDicts)
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
        raise
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    if zfsobj_db is not None:
        zfsobj_db.commit()
    return id

# persist-db____

#def lookup(ost_dbs, ostIdx, objId):
#    ost_zfsobj_db = ost_dbs[ostIdx]
#    query = 'select id, from_id, to_id, zfs_type from fatzap where ' \
#            'from_id=' + str(objId) + ' and zfs_type="Regular File"'
def lookup(ost_dbs, ostIdx, fid):
    ost_zfsobj_db = ost_dbs[ostIdx]
    # fid.rsplit(':',1)[0] trims off the version, which is different for each
    # ost stripe of the FID with a 0x0 version.

    partialfid = fid.rsplit(':',1)[0] + ':%'
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
    zfsobj_cursor.execute('SELECT id, path, uid, gid, ctime, mtime, atime, mode, objType, size, trustedFid, trustedLov, fid FROM zfsobj where fid like "' + partialfid + '"')
    all_row = zfsobj_cursor.fetchall()
    zfsobj_cursor.close()

    if len(all_row) > 1:
        print('More than one partial fid match to [', partialfid, '] in ost index', ostIdx)

    size_of_stripes_on_ost = 0
    for zfsobj_row in all_row:
        (id, path, uid, gid, ctime, mtime, atime, mode, objType, size, trustedFid, trusedLov, fid) = zfsobj_row
        size_of_stripes_on_ost = size_of_stripes_on_ost + size

    return size_of_stripes_on_ost

def getTotalSize(ost_dbs, parsed_lov):
    totalSize = 0
    try:
        parsed_raw = parsed_lov['ost_index_objids']
        try:
            ost_index_objids = map(
                lambda tup: (int(tup[0]), int(tup[1])),
                parsed_raw)
# This is where we can make client calls to lookup sizes in parallel...
            for lovOstIdx, lovObjIdx in ost_index_objids:
#                totalSize = totalSize + lookup(ost_dbs, lovOstIdx,
#                                               lovObjIdx)

                fid = hex(int(parsed_lov['lmm_seq'],16)) + ':' + \
                      hex(int(parsed_lov['lmm_object_id'],16)) + ':0x0'
                totalSize = totalSize + lookup(ost_dbs, lovOstIdx, fid)
        except:
            print(parsed_raw)
            raise
    except:
        raise
    return totalSize

def persistObjects(meta_db, mdt_dbs, ost_dbs):
    count = 0
    start = time.clock()
    meta_cur = meta_db.cursor()
    for mdtDatasetId, mdtDatasetDb in mdt_dbs.items():
        query = 'SELECT id, path, uid, gid, ctime, mtime, atime, mode,' \
                ' objType, size, trustedFid, trustedLov, fid FROM zfsobj'
        mdt_cursor = mdtDatasetDb.cursor()
        mdt_cursor.execute(query)
        mdt_curr_row = mdt_cursor.fetchone()
        while mdt_curr_row is not None:
            try:
                (id, path, uid, gid, ctime, mtime, atime, mode, objType, size, trustedFid, trustedLov, fid) = mdt_curr_row
                if trustedLov is not None and objType == 'ZFS plain file':
                    count = count + 1
                    if (count % 5000 == 0):
                        meta_db.commit()
                        meta_cur = meta_db.cursor()
                        ts = time.clock()
                        showTiming(count, start, ts)
                    path = path.lstrip('/ROOT')
                    parsed_lov = lovinfo.parseLovInfo(binascii.hexlify(str(fidinfo.decoder(trustedLov))))
                    # todo: MDT FID decoding currently experimental, add tests
                    # fid = '0x' + parsed_lov['lmm_seq'] + ':0x' + parsed_lov['lmm_object_id'] + ':0x0'
                    fid = hex(int(parsed_lov['lmm_seq'],16)) + ':' + \
                          hex(int(parsed_lov['lmm_object_id'],16)) + ':0x0'

                    # fid = ''
                    size = getTotalSize(ost_dbs, parsed_lov)
                    type = 'f' # todo: only regular files currently supported
                    metadata.save_metadata_obj(meta_cur, mdtDatasetId, path, uid, gid, ctime, mtime, atime, mode, type, size, fid)
            except:
                print('fail', mdt_curr_row)
                raise
            mdt_curr_row = mdt_cursor.fetchone()
        mdt_cursor.close()
    print('total ' + str(count))
    print('comitting')
    meta_db.commit()
    print('closing')
    meta_cur.close()
    print('done')

def persist(zesterDbFname, mdt_dbs, ost_dbs):
    print('persisting objects')
    meta_db = sqlite3.connect(zesterDbFname)
    meta_db.text_factory = str
    metadata.setup_metadata_db(meta_db)
    persistObjects(meta_db, mdt_dbs, ost_dbs)
    print('bulding metadata indexes')
    meta_cur = meta_db.cursor()
    meta_db.commit()
    meta_cur.close()
    meta_db.close()

def parse(filePaths):
    mdt_dbs = {}
    ost_dbs = {}
    for name in filePaths:
        start = time.clock()
        lustre_type, rest = name.split("_")
        pair = rest.split(".")
        id = int(pair[0])
        dump_type = pair[1]
        print('processing file: ' + name)
        zfsobjDbFname = lustre_type + '_' + str(id) + '.db'
        zfsobj_db = setup_zfsobj_db(zfsobjDbFname)
        if dump_type != "zdb":
            raise Exception("only zdb dumps currently supported")
        count = parseZdb(id, fileinput.input([name]), zfsobj_db)
        zfsobj_cur = zfsobj_db.cursor()
        zfsobj_cur.execute('create index zfsobj_trustedlov_index on '
                           'zfsobj (trustedLov)')
#        zfsobj_cur.execute('create index fatzap_from_id_index on fatzap '
#                           '(from_id)')
        zfsobj_db.commit()
        ts = time.clock()
        showTiming(count, start, ts)
        if lustre_type == 'mdt':
            mdt_dbs[id] = zfsobj_db
        elif lustre_type == 'ost':
            ost_dbs[id] = zfsobj_db
        else:
            raise Exception("must be ost or mdt dump")
    return mdt_dbs, ost_dbs

# main____

zesterDbFname  = 'metadata.db'

msg = '''Usage: zester [OPTION]... mdt_<mdtidx>.zdb ... ost_<ostidx>.zdb ...
Parse MDT and ZDB dumps into a SQLite representation then assemble into a queryable metadata.db SQLite DB file

Options:
 --parse         only parse ZDB dumps into SQLite DB, do not assemble
'''

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(msg)
        sys.exit(1)
    if sys.argv[1] == '--parse':
        mdt_dbs, ost_dbs = parse(sys.argv[2:])
    else:
        mdt_dbs, ost_dbs = parse(sys.argv[1:])
        persist(zesterDbFname, mdt_dbs, ost_dbs)
