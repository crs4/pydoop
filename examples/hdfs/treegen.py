#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, random
from pydoop.hdfs import hdfs
from common import isdir, MB, HOSTNAME, PORT, TEST_ROOT


BS_RANGE = [bs*MB for bs in range(50, 101, 10)]


def treegen(fs, root, depth, span):
  if isdir(fs, root) and depth > 0:
    for i in xrange(span):
      path = "%s/%d_%d" % (root, depth, i)
      kind = 'file' if i else 'directory'
      if kind == 'file':
        bs = random.sample(BS_RANGE, 1)[0]
        sys.stderr.write("%s %s %d\n" % (kind[0].upper(), path, (bs/MB)))
        f = fs.open_file(path, os.O_WRONLY, 0, 0, bs)
        f.write(path)
        f.close()
      else:
        sys.stderr.write("%s %s 0\n" % (kind[0].upper(), path))
        fs.create_directory(path)
        treegen(fs, path, depth-1, span)


def main(argv):
  
  try:
    depth = int(argv[1])
    span = int(argv[2])
  except IndexError:
    print "Usage: python %s DEPTH SPAN" % argv[0]
    sys.exit(2)

  fs = hdfs(HOSTNAME, PORT)
  root = "%s/%s" % (fs.working_directory(), TEST_ROOT)
  try:
    fs.delete(root)
  except IOError:
    pass
  fs.create_directory(root)
  treegen(fs, root, depth, span)
  fs.close()


if __name__ == "__main__":
  main(sys.argv)
