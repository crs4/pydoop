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
Traverse an HDFS tree and output disk space usage by block size.
"""
# DOCS_INCLUDE_START
import pydoop.hdfs as hdfs
from common import MB, TEST_ROOT


def usage_by_bs(fs, root):
    stats = {}
    for info in fs.walk(root):
        if info['kind'] == 'directory':
            continue
        bs = int(info['block_size'])
        size = int(info['size'])
        stats[bs] = stats.get(bs, 0) + size
    return stats


if __name__ == "__main__":
    with hdfs.hdfs() as fs:
        root = "%s/%s" % (fs.working_directory(), TEST_ROOT)
        print("BS(MB)\tBYTES")
        for k, v in usage_by_bs(fs, root).items():
            print("%.1f\t%d" % (k / float(MB), v))
