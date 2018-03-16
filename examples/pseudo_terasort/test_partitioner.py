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

import pickle
import io
from collections import Counter

import pydoop.hdfs as hdfs
from pterasort import Partitioner

RECORD_LENGTH = 91
KEY_LENGTH = 10

fname = Partitioner.initialize_break_points(
    5, 1000, '/user/root/genrecords_output'
)
with io.open('__break_point_cache_file', 'rb') as f:
    data = f.read()
sel = pickle.loads(data)

block_size = 20000 * RECORD_LENGTH
path = '/user/root/genrecords_output/part-m-00000'
with hdfs.open(path, 'rb') as f:
    data = f.read(block_size)
keys = (data[k:k + 10] for k in range(0, block_size, RECORD_LENGTH))
partitions = Counter(map(sel.select_partition, keys))
print(partitions)
