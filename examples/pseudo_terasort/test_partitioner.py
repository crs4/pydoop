import sys
from pterasort import Partitioner
import pydoop.hdfs as hdfs
import pickle
import io
from collections import Counter

RECORD_LENGTH = 91
KEY_LENGTH = 10

fname = Partitioner.initialize_break_points(5, 1000, '/user/root/genrecords_output')
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



