#!/usr/bin/env python2

# Copyright [2017], The Trustees of Indiana University. Licensed under the
# GNU General Public License Version 2 (see COPYING.TXT). You may not use
# this file except in compliance with the License. Unless required by
# applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the terms and
# conditions of the GPL2 License for more details.
#
# Written by Shawn Slavin for the High Performance File Systems group
# in the Pervasive Technology Institute at Indiana University.

from __future__ import print_function, division
import os
import random
import stat
import tempfile


def pickuid():
    randuid = random.randrange(1000, 10000)  # Pick uids from 1000 - 9999.
    return (randuid)


def pickgid():
    randgid = random.randrange(1000, 10000)  # Pick gids from 1000 - 9999.
    return (randgid)


def pickmode():
    modebits = (00, 01, 02, 03, 04, 05, 06, 07)  # Set of values possible in 3 bits
    u = random.choice(modebits) << 6
    g = random.choice(modebits) << 3
    o = random.choice(modebits)
    mode = u + g + o
    #    print( u, g, o, mode, oct(mode) )
    return (mode)


def generate(chownFiles):
    basepath = tempfile.mkdtemp(prefix='zester-tmp-', dir='.')
    print(basepath)
    total_dirs = 0
    total_files = 0
    for ilevel in range(nlevels):
        total_dirs = total_dirs + 1
        # print('nlevels = ', nlevels)
        basepath = tempfile.mkdtemp(prefix='zester-tmp-lvl-' + str(ilevel) + '-',
                                    dir=basepath)
        print(basepath)
        for idir in range(ndir_per_level):
            total_dirs = total_dirs + 1
            # print('ndir_per_level = ', ndir_per_level)
            dirpath = tempfile.mkdtemp(prefix='zester-dir-', dir=basepath)
            # print('dirpath = ', dirpath)
            nfiles = (int(random.random() * nfiles_per_dir_max
                          - nfiles_per_dir_min) + nfiles_per_dir_min)
            # print('nfiles = ', nfiles)
            for ifile in xrange(nfiles):
                total_files = total_files + 1
                ftype = random.choice(filetypes)
                fname = 'tmp{0:06d}'.format(random.randint(000000, 999999))
                path = os.path.join(dirpath, fname)
                print(path)
                while os.access(path, os.F_OK):
                    # print(path + ' exists. Selecting another random name...')
                    fname = 'tmp{0:06d}'.format(random.randint(000000, 999999))
                    path = os.path.join(dirpath, fname)
                    # print('ftype, path = ', ftype, path)
                perms = pickmode()
                owner_id = pickuid()
                group_id = pickgid()
                # Need to decide about randomizing permissions
                if ftype == 'f':  # A regular file
                    #            os.mknod(path, perms|stat.S_IFREG)
                    nbytes = (int(random.random() * nbytes_per_file_max
                                  - nbytes_per_file_min) + nbytes_per_file_min)
                    # print('nbytes = ', nbytes)
                    with open(path, 'w') as fh:
                        fh.write(os.urandom(nbytes))
#                        fh.write(nbytes*'0')
                        fh.close()
                    if chownFiles:
                        os.chown(path, owner_id, group_id)
                elif ftype == 'l':  # A symbolic link
                    os.symlink('/dev/null', path)
                elif ftype == 'p':  # A pipe or FIFO; can also use os.mkfifo()
                    os.mknod(path, perms | stat.S_IFIFO)
                    os.chown(path, owner_id, group_id)
                elif ftype == 'b':  # A block device file
                    os.mknod(path, perms | stat.S_IFBLK)
                elif ftype == 'c':  # A character device file
                    os.mknod(path, perms | stat.S_IFCHR)
                elif ftype == 's':  # A socket device file
                    os.mknod(path, perms | stat.S_IFSOCK)
    print('total_files', total_files)
    print('total_dirs', total_dirs)

# log = tempfile.mkstemp(suffix='', prefix='log.', dir=None, text=False)

# todo: make below into function parameters
byte = 1
kilobyte = 1024
megabyte = 1024 * kilobyte
gigabyte = 1024 * megabyte

nlevels = 5
ndir_per_level = 5
nfiles_per_dir_min = 10
nfiles_per_dir_max = 10
nbytes_per_file_min = 4000 * byte
nbytes_per_file_max = 6000 * byte

# Set up a list of filetype codes, where the frequency in the list shows the
# relative proportion of one type to the others in the randomly selected types
# for output. In this case, make regular files ten times more likely than
# other more special types.
# filetypes += ['l', 'p', 'b', 'c', 's']
filetypes = 10 * ['f']

if __name__ == '__main__':
    generate(False)
