# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, tempfile, os
from itertools import izip

import pydoop.hdfs as hdfs
import pydoop.hdfs_utils as hdfs_utils
from utils import make_random_data


class TestHDFSUtils(unittest.TestCase):

  def setUp(self):
    wd = tempfile.mkdtemp()
    wd_bn = os.path.basename(wd)
    self.local_wd = "file:%s" % wd
    fs = hdfs.hdfs("default", 0)
    fs.create_directory(wd_bn)
    self.hdfs_wd = fs.get_path_info(wd_bn)["name"]
    fs.close()
    basenames = ["test_path_%d" % i for i in xrange(2)]
    self.local_paths = ["%s/%s" % (self.local_wd, bn) for bn in basenames]
    self.hdfs_paths = ["%s/%s" % (self.hdfs_wd, bn) for bn in basenames]
    self.data = make_random_data(4*hdfs_utils.BUFSIZE + hdfs_utils.BUFSIZE/2)

  def tearDown(self):
    fs = hdfs.hdfs("", 0)
    fs.delete(self.local_wd)
    fs.close()
    fs = hdfs.hdfs("default", 0)
    fs.delete(self.hdfs_wd)
    fs.close()

  def dump(self):
    print
    for test_path in self.hdfs_paths[0], self.local_paths[0]:
      print "  file: %s" % test_path
      hdfs_utils.dump(self.data, test_path)
      with hdfs.open(test_path) as fi:
        rdata = fi.read()
      fi.fs.close()
      self.assertEqual(rdata, self.data)

  def load(self):
    print
    for test_path in self.hdfs_paths[0], self.local_paths[0]:
      print "  file: %s" % test_path
      hdfs_utils.dump(self.data, test_path)
      rdata = hdfs_utils.load(test_path)
      self.assertEqual(rdata, self.data)

  def cp(self):
    print
    for src in self.hdfs_paths[0], self.local_paths[0]:
      print "  src: %s" % src
      hdfs_utils.dump(self.data, src)
      for dest in self.hdfs_paths[1], self.local_paths[1]:
        print "    dest: %s" % dest
        hdfs_utils.cp(src, dest)
        with hdfs.open(dest) as fi:
          rdata = fi.read()
        self.assertEqual(rdata, self.data)

  def __ls(self, ls_func, path_transform):
    print
    for wd, paths in izip(
      (self.local_wd, self.hdfs_wd), (self.local_paths, self.hdfs_paths)
      ):
      print "  dir: %s" % wd
      for p in paths:
        hdfs_utils.dump(self.data, p)
        self.assertEqual(path_transform(ls_func(p)[0]), p)
      dir_list = [path_transform(p) for p in ls_func(wd)]
      self.assertEqual(set(dir_list), set(paths))

  def lsl(self):
    self.__ls(hdfs_utils.lsl, lambda x: x["name"])

  def ls(self):
    self.__ls(hdfs_utils.ls, lambda x: x)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestHDFSUtils("dump"))
  suite.addTest(TestHDFSUtils("load"))
  suite.addTest(TestHDFSUtils("cp"))
  suite.addTest(TestHDFSUtils("lsl"))
  suite.addTest(TestHDFSUtils("ls"))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
