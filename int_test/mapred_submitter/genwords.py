#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2020 CRS4.
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

import argparse
import io
import os
import sys
from random import choice


POOL = b"""\
lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod
tempor incididunt ut labore et dolore magna aliqua ut enim ad minim veniam
quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
consequat duis aute irure dolor in reprehenderit in voluptate velit esse
cillum dolore eu fugiat nulla pariatur excepteur sint occaecat cupidatat non
proident sunt in culpa qui officia deserunt mollit anim id est laborum
""".splitlines(True)


def genfile(path, size):
    current_size = 0
    with io.open(path, "wb") as f:
        while current_size < size:
            line = choice(POOL)
            f.write(line)
            current_size += len(line)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("out_dir", metavar="OUT_DIR")
    parser.add_argument("--n-files", metavar="INT", type=int, default=2)
    parser.add_argument("--file-size", metavar="BYTES", type=int, default=1000)
    args = parser.parse_args(sys.argv[1:])
    os.makedirs(args.out_dir)
    for i in range(args.n_files):
        path = os.path.join(args.out_dir, "f%d.txt" % i)
        genfile(path, args.file_size)
