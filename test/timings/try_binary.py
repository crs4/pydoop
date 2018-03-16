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
import sys
sys.path.insert(0, '../../build/lib.linux-x86_64-2.7')

from pydoop.mapreduce.binary_streams import (
    BinaryDownStreamFilter, BinaryWriter
)

from timer import Timer


def test_write(N, fname):
    with open(fname, 'w') as f:
        writer = BinaryWriter(f)

        def foo():
            writer_send = writer.send
            while True:
                key, val = yield True
                writer_send("mapItem", key, val)
        foo_fg = foo()
        next(foo_fg)
        for i in range(N):
            foo_fg.send(("key", "val"))


def write_data(N, fname):
    with open(fname, 'w') as f:
        writer = BinaryWriter(f)
        for i in range(N):
            writer.send('mapItem', "key", "val")
        writer.send('close')


def read_data(fname, N=None):
    with open(fname, 'rb', buffering=(4096 * 4)) as f:
        reader = BinaryDownStreamFilter(f)
        if N is None:
            for cmd, args in reader:
                pass
        else:
            for i in range(N):
                cmd, args = next(reader)


def main():
    fname = 'foo.dat'
    N = 100000
    with Timer() as t:
        write_data(N, fname)
    print("=> write_data: %s s" % t.secs)

    with Timer() as t:
        test_write(N, fname)
    print("=> test_write(): %s s" % t.secs)

    with Timer() as t:
        read_data(fname)
    print("=> read_data: %s s" % t.secs)
    with Timer() as t:
        read_data(fname, 100000)
    print("=> read_data(100000): %s s" % t.secs)
    with Timer() as t:
        read_data(fname, 50000)
    print("=> read_data(50000): %s s" % t.secs)

    with open(fname, 'rb', buffering=(4096 * 4)) as f:
        reader = BinaryDownStreamFilter(f)
        for i in range(10):
            cmd, args = next(reader)
            print(cmd, args)


main()
