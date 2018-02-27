# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

from __future__ import print_function
from pydoop.mapreduce.binary_streams import (
    BinaryWriter, BinaryDownStreamFilter
)

import sys

sys.path.append('./build/lib.linux-x86_64-2.7')

import dummy

from timer import Timer


def write_data(N, fname):
    with open(fname, 'w') as f:
        writer = BinaryWriter(f)
        for i in range(N):
            writer.send('mapItem', "key", "val")
        writer.send('close')


# At system level, the following two routines are equivalent:
# strace claims that they do the same set of buffered (4096 bytes) system read.
# So, io is buffered and the buf size is 4096.

# @profile
def read_data(fname):
    with open(fname, 'rb', buffering=(4096 * 4)) as f:
        reader = BinaryDownStreamFilter(f)
        for cmd, args in reader:
            pass


def read_data1(N, fname):
    with open(fname, 'rb', buffering=(4096 * 4)) as f:
        for i in range(N):
            dummy.decode_command(f)


def read_data2(N, fname):
    with open(fname, 'rb', buffering=(4096 * 4)) as f:
        dummy.decode_command(f, N)


def main():
    fname = 'foo.dat'
    N = 100000
    with Timer() as t:
        write_data(N, fname)
    print("=> write_data: %s s" % t.secs)
    # read_data_minimal(fname)
    with Timer() as t:
        read_data(fname)
    print("=> read_data: %s s" % t.secs)
    with Timer() as t:
        read_data1(N, fname)
    print("=> read_data1: %s s" % t.secs)
    with Timer() as t:
        read_data2(N, fname)
    print("=> read_data2: %s s" % t.secs)


main()
