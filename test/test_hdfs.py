# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, tempfile, os
from itertools import izip

import pydoop.hdfs as hdfs
import pydoop.hdfs.config as hconf
from utils import make_random_data


class TestHDFS(unittest.TestCase):

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
    self.data = make_random_data(4*hconf.BUFSIZE + hconf.BUFSIZE/2)
    for path in self.local_paths:
      self.assertTrue(path.startswith("file:"))
    for path in self.hdfs_paths:
      self.assertTrue(path.startswith("hdfs:"))

  def tearDown(self):
    fs = hdfs.hdfs("", 0)
    fs.delete(self.local_wd)
    fs.close()
    fs = hdfs.hdfs("default", 0)
    fs.delete(self.hdfs_wd)
    fs.close()

  def open(self):
    for test_path in self.hdfs_paths[0], self.local_paths[0]:
      with hdfs.open(test_path, "w") as f:
        f.write(self.data)
      f.fs.close()
      with hdfs.open(test_path) as f:
        self.assertEqual(f.read(), self.data)
      f.fs.close()

  def dump(self):
    for test_path in self.hdfs_paths[0], self.local_paths[0]:
      hdfs.dump(self.data, test_path)
      with hdfs.open(test_path) as fi:
        rdata = fi.read()
      fi.fs.close()
      self.assertEqual(rdata, self.data)

  def load(self):
    for test_path in self.hdfs_paths[0], self.local_paths[0]:
      hdfs.dump(self.data, test_path)
      rdata = hdfs.load(test_path)
      self.assertEqual(rdata, self.data)

  def cp(self):
    for src in self.hdfs_paths[0], self.local_paths[0]:
      hdfs.dump(self.data, src)
      for dest in self.hdfs_paths[1], self.local_paths[1]:
        hdfs.cp(src, dest)
        with hdfs.open(dest) as fi:
          rdata = fi.read()
        self.assertEqual(rdata, self.data)

  def __ls(self, ls_func, path_transform):
    for wd, paths in izip(
      (self.local_wd, self.hdfs_wd), (self.local_paths, self.hdfs_paths)
      ):
      for p in paths:
        hdfs.dump(self.data, p)
        self.assertEqual(path_transform(ls_func(p)[0]), p)
      dir_list = [path_transform(p) for p in ls_func(wd)]
      self.assertEqual(set(dir_list), set(paths))

  def lsl(self):
    self.__ls(hdfs.lsl, lambda x: x["name"])

  def ls(self):
    self.__ls(hdfs.ls, lambda x: x)

  def mkdir(self):
    for wd in self.local_wd, self.hdfs_wd:
      d1 = "%s/d1" % wd
      d2 = "%s/d2" % d1
      hdfs.mkdir(d2)
      dir_list = hdfs.ls(d1)
      self.assertEqual(len(dir_list), 1)
      self.assertTrue(dir_list[0].endswith(d2))

  def rmr(self):
    for wd in self.local_wd, self.hdfs_wd:
      d1 = "%s/d1" % wd
      d2 = "%s/d2" % d1
      hdfs.mkdir(d2)
      for d, bn in ((d1, "f1"), (d2, "f2")):
        hdfs.dump(self.data, "%s/%s" % (d, bn))
      hdfs.rmr(d1)
      self.assertEqual(len(hdfs.ls(wd)), 0)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestHDFS("open"))
  suite.addTest(TestHDFS("dump"))
  suite.addTest(TestHDFS("load"))
  suite.addTest(TestHDFS("cp"))
  suite.addTest(TestHDFS("lsl"))
  suite.addTest(TestHDFS("ls"))
  suite.addTest(TestHDFS("mkdir"))
  suite.addTest(TestHDFS("rmr"))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
