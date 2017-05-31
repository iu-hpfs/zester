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
    if hexLov[5] == '3':
        poolInUse = True
        offset = 97
    else:
        poolInUse = False
        offset = 65

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# fields 8 - 16 UNKNOWN
# no de-reference code, as we dont know what the hell this is
#
# fields 17 - 22 are the lmm object id
# order is 21,22,19,20,17,18

    lmmObjId = []
    for i in [ 20, 21, 18, 19, 16, 17 ]:
        lmmObjId.append(hexLov[i])

# Join list elements, in order, resulting in a string.
    lmmObjId = ''.join(lmmObjId)

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# fields 21 - 32 UNKNOWN
# fields 33 - 42 are the lmm seq
# order is 41,42,39,40,37,38,35,36,33,34

    lmmSeq = []
    for i in [ 40, 41, 38, 39, 36, 37, 34, 35, 32, 33 ]:
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

    tmpSize = []
    for i in [ 54, 55, 52, 53 ]:
        tmpSize.append(hexLov[i])

    tmpSize = ''.join(tmpSize)

   # multiply stripe size by 64k, return value is bytes in decimal
    lmmStripeSize = str(int(tmpSize, 16) * 65536)

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# field 57 - 58 is stripe width in hex
# not sure if field is greater than 2
# do not have a working example of an file striped creater than 255
#
# Note that lmmStripeCount is still hexadecimal for now...
    lmmStripeCount = hexLov[56:58]  # Include hexLov[56] and hexLov[57].
# Apparently not byte swapped?
#    for i in [ 57, 56 ]:
#        lmmStripeCount.append(hexLov[i])

# Join list elements, in order, resulting in a string.
    lmmStripeCount = ''.join(lmmStripeCount)
#    lmmStripeCount = str(int(lmmStripeCount, 16)) # Convert to decimal

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# GET_LMM_POOL() {
#    # 32 bit field, 16 hex values
#    # position 65 - 96

    lmmPool = ''

    if poolInUse:
        # Note: Tom's script doesn't byte swap this segment
        lmmPoolValue = hexLov[64:96]   # Include hexLov[64] through hexLov[95].

        for istart in range(0,32,2):
            iend = istart + 1
            # Include lmmPoolValue[istart] through lmmPoolValue[iend].
            nextByte = int(str(lmmPoolValue[istart:iend+1]),16)
            lmmPool += chr(nextByte)
        lmmPool = lmmPool.strip()
        lmmPool = lmmPool.rstrip('\x00')

# From get-lov-info.sh (note field #'s are unit-offset in the commentary):
# GET_OST_OBJID() {
# offset depends on if pool's are used is 47  [Not certain what Tom meant here]
# 97 w/pool, 65 w/out pool

    ostObjId = ''
    recordNum = 0
    recordWidth = 47

# Loop through records for ostObjId values.
# [Remember lmmStripeCount is a string value for a hexidecimal number.]
    while recordNum < int(lmmStripeCount, 16):

        recordNum += 1
        recordOffset = recordWidth * recordNum

# Recall that offset is already zero-offset. So, no modification to algorithm
# needed here.
# Corrects for zero-offset addressing by subtracting 1 from computed iend value
        iend = offset + recordOffset - 1
        istart = iend - recordWidth

        record = hexLov[istart:iend+1]

# ostIndex lives in 42nd element of record
        ostIndex=record[41]

# Pull out ostObjId
        ostObjIdValue = ''

        for i in [4, 5, 2, 3, 0, 1]:
            ostObjIdValue += record[i]

        offset += 1

        ostObjIdValueDecimal = str(int(ostObjIdValue, 16))
        ostObjId += (' ' + ostIndex + ' ' + ostObjIdValueDecimal)

    return {
        'lmm_magic':lmmMagic,
        'lmm_seq':lmmSeq,
        'lmm_object_id':lmmObjId,
        'lmm_stripe_count':lmmStripeCount,
        'lmm_stripe_size':lmmStripeSize,
        'lmm_pool':lmmPool,
        'ost_index_objids':list(partition_iter(ostObjId.strip().split(), 2)) }
