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

import argparse
import io
import os
import sys


RECORD_LEN = 100
BUFSIZE = 32 * 1024


def iterrecords(f):
    while True:
        record = f.read(RECORD_LEN)
        if not record:
            raise StopIteration
        yield record


def check_record(i, record):
    assert len(record) == RECORD_LEN
    assert record[10:12] == b"\x00\x11"
    assert int(record[12:44], 16) == i
    assert record[44:48] == b"\x88\x99\xaa\xbb"
    assert record[-4:] == b"\xcc\xdd\xee\xff"


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("fname", metavar="FILE")
    args = parser.parse_args(argv[1:])
    size = os.stat(args.fname).st_size
    n_records, r = divmod(size, RECORD_LEN)
    assert r == 0
    report_step = n_records // 10
    with io.open(args.fname, "rb", buffering=BUFSIZE) as f:
        for i, record in enumerate(iterrecords(f)):
            check_record(i, record)
            if i % report_step == 0:
                print('*', end='')
                sys.stdout.flush()
        print()
    assert i + 1 == n_records
    print("n. records: %d" % n_records)


if __name__ == "__main__":
    main(sys.argv)
