#!/usr/bin/env python2

# Copyright [2017], The Trustees of Indiana University. Licensed under the
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

import itertools
import functools


def partition_iter(iterator, partition_size):
    iterator = iter(iterator)
    return iter(lambda: tuple(itertools.islice(iterator, partition_size)), ())


# --------------------------------------------------------------------------- #
# parseLovInfo(): Pythonic function to mimic THC's script
# Note: This is rather monolithic, whereas THC's script encapsulates functions
# for each value. I am thinking we may want to use a class structure for
# the TrustedLov object, where those functions, as written here, would be
# methods for the object class.
# 2016-11-18 SDS
def parseLovInfo(hexLov):

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# fields 1 - 8 are the lmm magic
# order is fields 7,8,5,6,3,4,1,2
# 6th field seems to indicate if a pool is used or not:
# 3 means "pool in use".
# 1 means "no pool"

    lmmMagic = []
    for i in [ 6, 7, 4, 5, 2, 3, 0, 1 ]:
        lmmMagic.append(hexLov[i])

# Join list elements, in order, resulting in a string.
    lmmMagic = ''.join(lmmMagic)

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# value of 3 seems to indicate pool's are in use, which injects a 32-character
# field before ost information, have to adjust OFFSET accordingly
#
# Note: The offsets are what they are, relative to the first element of hexLov.
# I've corrected for zero-offset addressing wherever an offset is used.
# [SDS, 11/23/16]
#
# #define LOV_USER_MAGIC_V1       0x0BD10BD0
# #define LOV_USER_MAGIC_V3       0x0BD30BD0
# Recall that hexLov is little-endian, and so LOV_USER_MAGIC_V1 would be D00BD10B
# Where hexLov[5] corresponds to the difference between v1 (pool not in use) and v3
# (pool in use).
    if hexLov[5] == '3':
        poolInUse = True
        offset = 96
    else:
        poolInUse = False
        offset = 64

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# fields 8 - 16 UNKNOWN
# no de-reference code, as we dont know what the hell this is
#
# fields 17 - 22 are the lmm object id
# order is 21,22,19,20,17,18
#
# zero-offset order: 30, 31, 28, 29, 26, 27, 24, 25, 22, 23, 20, 21, 18, 19, 16, 17

    lmmObjId = []
    for i in [ 30, 31, 28, 29, 26, 27, 24, 25, 22, 23, 20, 21, 18, 19, 16, 17 ]:
        lmmObjId.append(hexLov[i])

# Join list elements, in order, resulting in a string.
    lmmObjId = ''.join(lmmObjId)

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# fields 21 - 32 UNKNOWN
# fields 33 - 42 are the lmm seq
# order is 41,42,39,40,37,38,35,36,33,34
#
# zero-offset order: 46, 47, 44, 45, 42, 43, 40, 41, 38, 39, 36, 37, 34, 35, 32, 33
    lmmSeq = []
    for i in [ 46, 47, 44, 45, 42, 43, 40, 41, 38, 39, 36, 37, 34, 35, 32, 33 ]:
        lmmSeq.append(hexLov[i])

# Join list elements, in order, resulting in a string.
    lmmSeq = ''.join(lmmSeq)

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# fields 53-56 are hex representations of stripe size. 64k smallest to almost 4GB largest. (0x0001 to 0xffff) * 65536 = stripe size in bytes
# order is 55,56,53,54.
#
# XXXX
# ||||
# |||\----> 16M to 255M
# ||\-----> 256M to 3840M
# |\------> 64k to 960k
# \-------> 1M to 15M
#
# 2018-05-07, SDS
# This decoding was a little bit confused because little-endian places 48 and 49 were ignored, making the assumption that the rest
# multiplied 64k, which is (I believe a minimum stripe size in Lustre). However, if you swab32() the places, then places 48 and 49
# fall into the 16^0 and 16^1 places in the hex number. So, unless those places ever hold non-zero values, the minimum hex value
# for stripe size would then be 0x000100, which would be 64k. However, I'm not sure we should ignore 48 and 49 in our decoding.
#
#    tmpSize = []
#    for i in [ 54, 55, 52, 53 ]:
#        tmpSize.append(hexLov[i])
#
#    tmpSize = ''.join(tmpSize)
#
#   # multiply stripe size by 64k, return value is bytes in decimal
#    lmmStripeSize = str(int(tmpSize, 16) * 65536)

    lmmStripeSize = []
    for i in [ 54, 55, 52, 53, 50, 51, 48, 49 ]:
        lmmStripeSize.append(hexLov[i])
    lmmStripeSize = ''.join(lmmStripeSize)

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# field 57 - 58 is stripe width in hex
# not sure if field is greater than 2
# do not have a working example of an file striped creater than 255
#
# Note that lmmStripeCount is still hexadecimal for now...
    lmmStripeCount = []
    for i in [ 58, 59, 56, 57 ]:
        lmmStripeCount.append(hexLov[i])
    lmmStripeCount = ''.join(lmmStripeCount)
#    lmmStripeCount = str(int(lmmStripeCount, 16)) # Convert to decimal

    lmmLayoutGeneration = []
    for i in [ 62, 63, 60, 61 ]:
        lmmLayoutGeneration.append(hexLov[i])
    lmmLayoutGeneration = ''.join(lmmLayoutGeneration)

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# GET_LMM_POOL() {
#    # 32 bit field, 16 hex values
#    # position 65 - 96

    lmmPool = ''

    if poolInUse:
        # Note: Tom's script doesn't byte swap this segment
        lmmPoolValue = hexLov[64:96]   # Include hexLov[64] through hexLov[95].

        for istart in range(0,32,2):
            istop = istart + 2
            # Include lmmPoolValue[istart] through lmmPoolValue[istop-1].
            nextByte = int(str(lmmPoolValue[istart:istop]),16)
            lmmPool += chr(nextByte)
        lmmPool = lmmPool.strip()
        lmmPool = lmmPool.rstrip('\x00')

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# GET_OST_OBJID() {
# offset depends on if pool's are used is 47  [Not certain what Tom meant here]
# 96 w/pool, 64 w/out pool

    ostObjId = ''

    recordWidth = 48
    
# Loop through records for ostObjId values.
# [Remember lmmStripeCount is a string value for a hexidecimal number.]

    for recordNum in range( int(lmmStripeCount, 16) ):

        istart = offset + recordNum * recordWidth
        istop = istart + recordWidth
        record = hexLov[istart:istop]

# ostIndex lives in 42nd element of record
# [not quite; 2018-05-07; it's u64:objid|u64:group|u32:gen|u32:ost/obdidx]
# so, using zero-offset notation in little endian, it would be
# [objid:group: gen :ostid]
# [00-15:16-31:32-39:40-47]


        fid = hex(int(lmmSeq,16)) + ':' + hex(int(lmmObjId,16)) + ':0x0'
        ostIndex = []
        for i in [ 46, 47, 44, 45, 42, 43, 40, 41 ]:
            ostIndex.append(record[i])
        ostIndex = ''.join(ostIndex)
        ostIndex = str(int(ostIndex, 16))


# For the record, I am completely confused about whether the first 8 bytes
# is the group or objid value. The following seems to produce results
# consistent with lfs getstripe output, but I can't figure out why.

# Pull out ostObjId
# Some magic happens when the lfs getstripe 'group' values are non-zero, which
# occurs when the files are located on an MDT other than MDT0. In that case,
# using the group value bytes gives us the correct object indices on the OSTs.
# If the group value is zero, the correct object indices are those in the
# second u64/8-byte objid value (as you might expect). So, check the first, and
# decide whether to go with the latter. There must be more dark magic I haven't
# seen yet. See my Dropbox/Work/trusted.lov.notes file. [2018-05-07, SDS]
        ostObjIdValue = []
        for i in [30, 31, 28, 29, 26, 27, 24, 25, 22, 23, 20, 21, 18, 19, 16, 17]:
            ostObjIdValue.append(record[i])
        ostObjIdValue = ''.join(ostObjIdValue)
        ostObjIdValueDecimal = str(int(ostObjIdValue, 16))

        if (ostObjIdValueDecimal == '0'):
            ostObjIdValue = []
            for i in [14, 15, 12, 13, 10, 11, 8, 9, 6, 7, 4, 5, 2, 3, 0, 1]:
               ostObjIdValue.append(record[i])
            ostObjIdValue = ''.join(ostObjIdValue)
            ostObjIdValueDecimal = str(int(ostObjIdValue, 16))

        ostObjId += (' ' + ostIndex + ' ' + ostObjIdValueDecimal)

    return {
        'lmm_magic':lmmMagic,
        'lmm_seq':lmmSeq,
        'lmm_object_id':lmmObjId,
        'lmm_stripe_count':lmmStripeCount,
        'lmm_stripe_size':lmmStripeSize,
        'lmm_pool':lmmPool,
        'ost_index_objids':list(partition_iter(ostObjId.strip().split(), 2))}
