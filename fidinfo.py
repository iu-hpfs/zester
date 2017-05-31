#!/usr/bin/env python2

# This file is based on the zfsobj2fid script, part of
# lustre-tools-llnl, currently including only the parts
# needed for Zester. The full zfsobj2fid script as well as the
# full lustre-tools-llnl distribution is distributed with
# Zester in the "lustre-tools-llnl.zip" archive.

# --------------------------------------------------------------------------- #
# Copyright (c) 2014, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory.
# Written by Christopher J. Morrone <morrone2@llnl.gov>
# LLNL-CODE-468512
#
# This file is part of lustre-tools-llnl.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License (as published by the
# Free Software Foundation) version 2, dated June 1991.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the IMPLIED WARRANTY OF
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# terms and conditions of the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

import re

# --------------------------------------------------------------------------- #
# Globally define this compiled regexp once for routine decoder.
# Note the need for the r'' syntax for a Pythonic "raw string" so that the
# backslash, or escape character, is passed correctly in the regular
# expression.

escapedOctet = re.compile( r'\\[0-7]{3}' )

def from_bytes(b):
    return sum(b[i] << i * 8
               for i in range(len(b)))

# --------------------------------------------------------------------------- #
def decoder(fid):
    b = bytearray()
    while len(fid) > 0:
        match = escapedOctet.search( fid[0:4] )
        if match:
            val = fid[1:4]
            fid = fid[4:]
            b.append(int(val, 8))
        else:
            val = fid[0]
            fid = fid[1:]
            b.append(ord(val))
    return b

# --------------------------------------------------------------------------- #
def decode_fid(octal_fid):
    b = decoder(octal_fid)
    return hex(from_bytes(b[0:8])) + ':' + hex(from_bytes(b[8:12])) + ':' + hex(from_bytes(b[12:16]))
