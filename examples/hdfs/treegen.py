# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

"""
Generate an HDFS tree containing files of different block size.
"""

import sys
import random

import pydoop
import pydoop.hdfs as hdfs

from common import isdir, MB, TEST_ROOT


BS_RANGE = [_ * MB for _ in range(50, 101, 10)]


def treegen(fs, root, depth, span):
    if isdir(fs, root) and depth > 0:
        for i in range(span):
            path = u"%s/%d_%d" % (root, depth, i)
            kind = 'file' if i else 'directory'
            if kind == 'file':
                kwargs = {}
                if pydoop.hadoop_version_info().has_deprecated_bs():
                    bs = hdfs.fs.hdfs().default_block_size()
                else:
                    bs = random.sample(BS_RANGE, 1)[0]
                    kwargs['blocksize'] = bs
                sys.stderr.write(
                    "%s %s %d\n" % (kind[0].upper(), path, (bs / MB))
                )
                with fs.open_file(path, "wt", **kwargs) as f:
                    f.write(path)
            else:
                sys.stderr.write("%s %s 0\n" % (kind[0].upper(), path))
                fs.create_directory(path)
                treegen(fs, path, depth - 1, span)


def main(argv):

    try:
        depth = int(argv[1])
        span = int(argv[2])
    except IndexError:
        print("Usage: python %s DEPTH SPAN" % argv[0])
        sys.exit(2)

    fs = hdfs.hdfs()
    try:
        root = "%s/%s" % (fs.working_directory(), TEST_ROOT)
        try:
            fs.delete(root)
        except IOError:
            pass
        fs.create_directory(root)
        treegen(fs, root, depth, span)
    finally:
        fs.close()


if __name__ == "__main__":
    main(sys.argv)
