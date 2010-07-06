#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys
from pydoop.hdfs import hdfs
from common import isdir, MB, HOSTNAME, PORT, TEST_ROOT


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


def main(argv):
  fs = hdfs(HOSTNAME, PORT)
  root = "%s/%s" % (fs.working_directory(), TEST_ROOT)
  if not isdir(fs, root):
    sys.exit("%r does not exist" % root)
  print "BS(MB)\tBYTES"
  for k, v in usage_by_bs(fs, root).iteritems():
    print "%.1f\t%d" % (k/float(MB), v)  
  fs.close()


if __name__ == "__main__":
  main(sys.argv)
