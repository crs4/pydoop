# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Traverse an HDFS tree and output disk space usage by block size.
"""

import sys
import pydoop.hdfs as hdfs
from common import isdir, MB, TEST_ROOT


def treewalker(fs, root_info):
  yield root_info
  if root_info['kind'] == 'directory':
    for info in fs.list_directory(root_info['name']):
      for item in treewalker(fs, info):
        yield item


def usage_by_bs(fs, root):
  stats = {}
  root_info = fs.get_path_info(root)
  for info in treewalker(fs, root_info):
    if info['kind'] == 'directory':
      continue
    bs = int(info['block_size'])
    size = int(info['size'])
    stats[bs] = stats.get(bs, 0) + size
  return stats


def main():
  fs = hdfs.hdfs()
  try:
    root = "%s/%s" % (fs.working_directory(), TEST_ROOT)
    if not isdir(fs, root):
      sys.exit("%r does not exist" % root)
    print "BS(MB)\tBYTES"
    for k, v in usage_by_bs(fs, root).iteritems():
      print "%.1f\t%d" % (k/float(MB), v)  
  finally:
    fs.close()


if __name__ == "__main__":
  main()
