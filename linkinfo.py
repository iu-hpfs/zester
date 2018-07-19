#!/usr/bin/env python2

# Copyright [2018], The Trustees of Indiana University. Licensed under the
# GNU General Public License Version 2 (see included COPYING.TXT). You
# may not use this file except in compliance with the License. Unless
# required by applicable law or agreed to in writing, software distributed
# under the  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, either express or implied. See the terms and
# conditions of the GPL2 License for more details.
#
# Written by Shawn Slavin, Tom Crowe, & Kenrick Rawlings for the High
# Performance File Systems group in the Pervasive Technology Institute at
# Indiana University.

# Example of trusted.link for a directory file called 'striped'
# From struct linkea_header,
# defined in <lustre-src-root>/lustre/include/lustre/lustre_idl.h:
# link header
#         __u32 leh_magic;
#         __u32 leh_reccount;
#         __u64 leh_len;      /* total size */
#         /* future use */
#         __u32 padding1;
#         __u32 padding2;
# From struct linkea_entry,
# defined in <lustre-src-root>/lustre/include/lustre/lustre_idl.h:
# link entry
#         unsigned char      lee_reclen[2];
#         unsigned char      lee_parent_fid[sizeof(struct lu_fid)];
#         char               lee_name[0];
#
# From linkea_entry_unpack(),
# defined in <lustre-src-root>/lustre/obdclass/linkea.c
#
# ln_namelen = *reclen - sizeof(struct link_ea_entry);
#
# Sample code shows that sizeof(struct link_ea_entry) = 18
#
# In this example, *reclen = 25
# so 25 - 18 = 7, which is the length of filename, where
#
# filename = 'striped'
#
# Breakdown of trusted.link for directory 'striped' in zdb output:
# trusted.link = \337\361\352\021\001\000\000\0001\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\031\000\000\000\002\000\000\000\007\000\000\000\001\000\000\000\000striped
#
#
# Little-endian octal | Big-endian octal (except FIDs which are left LE) | BE hex or dec:
# ../include/lustre/lustre_idl.h:#define LINK_EA_MAGIC 0x11EAF1DFUL
# magic:                  \337\361\352\021                 \021\352\361\337                 = 0x11eaf1df
# hard link record count: \001\000\000\000                 \000\000\000\001                 = 1
# header length:          1\000\000\000\000\000\000\000    \000\000\000\000\000\000\000\061 = 49
# padding 1:              \000\000\000\000                 \000\000\000\000
# padding 2:              \000\000\000\000                 \000\000\000\000
# record length:          \000\031                 ------> \000\031                         = 25
# parent fid sequence:    \000\000\000\002\000\000\000\007 0x0000000200000007 = 0x200000007
# parent fid objid:       \000\000\000\001                 0x00000001         = 0x1
# parent fid version:     \000\000\000\000                 0x00000000         = 0x0
# filename:               striped
#
# Also, note that the lu_fid struct uses u64 for fid_seq; u32 for fid_oid;
# and u32 for fid_ver:
#
# Note: In <lustre-src-root>/lustre/include/lustre/lustre_user.h
# struct lu_fid {
#        /**
#         * FID sequence. Sequence is a unit of migration: all files (objects)
#         * with FIDs from a given sequence are stored on the same server.
#         * Lustre should support 2^64 objects, so even if each sequence
#         * has only a single object we can still enumerate 2^64 objects.
#         **/
#         __u64 f_seq;
#         /* FID number within sequence. */
#         __u32 f_oid;
#         /**
#          * FID version, used to distinguish different versions (in the sense
#          * of snapshots, etc.) of the same file system object. Not currently
#          * used.
#          **/
#         __u32 f_ver;
# };
#


def parse_link_info(trusted_link_hex):
    import binascii

    link_magic = []  # Four-byte integer
    for i in [6, 7, 4, 5, 2, 3, 0, 1]:
        link_magic.append(trusted_link_hex[i])

    link_magic = ''.join(link_magic)

    hard_link_count = []  # Four-byte integer
    for i in [14, 15, 12, 13, 10, 11, 8, 9]:
        hard_link_count.append(trusted_link_hex[i])

    hard_link_count = ''.join(hard_link_count)
    hard_link_count = int(hard_link_count, 16)

    # Some debugging output
    print('Link EA magic:   {0:s}'.format(hex(int(link_magic, 16))))
    print('Hard Link Count: {0:d}'.format(hard_link_count))

    # Skip one eight-byte integer that holds the header length:
    # trusted_link_hex[16:32]
    #
    # Skip two four-byte integers of padding: trusted_link_hex[32:40] and
    # trusted_link_hex[40:48]
    #
    # Pick up on trusted_link_hex[48]...

    j = 48  # j is the place holder for indexing, as we loop through records

    # Return value will be a list of dicts with 'pfid' and 'filename' keys
    results = []

    # Loop over parsing of record length, parent FID, and filename.
    for ilink in range(hard_link_count):

        record_length = []  # Two-byte integer; not swapped
        # for i in [48, 49, 50, 51]:
        for i in range(4):
            record_length.append(trusted_link_hex[i + j])
        record_length = ''.join(record_length)
        record_length = int(record_length, 16)
        j = j + 4

        # Parse FID sequence, object_id, and version.
        # NOTE: Recall that FID info is left in little-endian format
        parent_fid_seq = []  # Eight-byte integer
        # for i in [52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67]:
        for i in range(16):
            parent_fid_seq.append(trusted_link_hex[i + j])
        parent_fid_seq = ''.join(parent_fid_seq)
        j = j + 16

        parent_fid_oid = []  # Four-byte integer
        for i in range(8):
            parent_fid_oid.append(trusted_link_hex[i + j])
        parent_fid_oid = ''.join(parent_fid_oid)
        j = j + 8

        parent_fid_ver = []  # Four-byte integer
        for i in range(8):
            parent_fid_ver.append(trusted_link_hex[i + j])
        parent_fid_ver = ''.join(parent_fid_ver)
        j = j + 8

        parent_fid = hex(int(parent_fid_seq, 16)) + ':' + hex(
            int(parent_fid_oid, 16)) + ':' + hex(int(parent_fid_ver, 16))

        # Remember that the link_ea_entry structure size is 18
        filename_length = record_length - 18
        # istart = 84
        istart = j
        istop = istart + 2 * filename_length
        j = istop

        filename = binascii.unhexlify(trusted_link_hex[istart:istop])

        # Some debugging output
        print('Parent FID:      [{0:s}]'.format(parent_fid))
        print('Record length:   {0:d}'.format(record_length))
        print('Filename length: {0:d}'.format(filename_length))
        print('Filename:        {0:s}'.format(filename))

        results.append({'pfid': parent_fid, 'filename': filename})

    return results


if __name__ == '__main__':
    import binascii

    trusted_link = '\337\361\352\021\002\000\000\000?\000\000\000\000\000\000\000\000\000\000\000' \
                   + '\000\000\000\000\000\023\000\000\000\002\000\000\004\000\000\000\000\002\000' \
                   + '\000\000\000a\000\024\000\000\000\002@\000\004\001\000\000\000\002\000\000\000' \
                   + '\000aa'

    trusted_link_hexlified = binascii.hexlify(trusted_link)

    results0 = parse_link_info(trusted_link_hexlified)

    print(results0)
