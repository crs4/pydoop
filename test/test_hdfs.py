# BEGIN_COPYRIGHT
# 
# Copyright 2009-2013 CRS4.
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

import unittest, tempfile, os
from itertools import izip
import stat

import pydoop.hdfs as hdfs
from pydoop.hdfs.common import BUFSIZE
from utils import make_random_data, FSTree


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
    self.data = make_random_data(4*BUFSIZE + BUFSIZE/2)
    for path in self.local_paths:
      self.assertTrue(path.startswith("file:"))
    for path in self.hdfs_paths:
      if not hdfs.default_is_local():
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

  def __ls(self, ls_func, path_transform):
    for wd, paths in izip(
      (self.local_wd, self.hdfs_wd), (self.local_paths, self.hdfs_paths)
      ):
      for p in paths:
        hdfs.dump(self.data, p)
      test_dir = "%s/%s" % (wd, "test_dir")
      test_path = "%s/%s" % (test_dir, "test_path")
      hdfs.dump(self.data, test_path)
      paths.append(test_dir)
      for recursive in False, True:
        if recursive:
          paths.append(test_path)
        dir_list = [path_transform(p) for p in ls_func(wd, recursive=recursive)]
        self.assertEqual(sorted(dir_list), sorted(paths))

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

  def load(self):
    for test_path in self.hdfs_paths[0], self.local_paths[0]:
      hdfs.dump(self.data, test_path)
      rdata = hdfs.load(test_path)
      self.assertEqual(rdata, self.data)

  def __make_tree(self, wd):
    d1 = "%s/d1" % wd
    t1 = FSTree(d1)
    d2 = "%s/d2" % d1
    t2 = t1.add(d2)
    hdfs.mkdir(d2)
    for t, d, bn in ((t1, d1, "f1"), (t2, d2, "f2")):
      f = "%s/%s" % (d, bn)
      hdfs.dump(self.data, f)
      t.add(f, 0)
    return t1

  def __cp_file(self, wd):
    fn = "%s/fn" % wd
    hdfs.dump(self.data, fn)
    dest_dir = "%s/dest_dir" % wd
    hdfs.mkdir(dest_dir)
    fn_copy_on_wd = "%s/fn_copy" % wd
    hdfs.cp(fn, fn_copy_on_wd)
    self.assertEqual(hdfs.load(fn_copy_on_wd), self.data)
    self.assertRaises(IOError, hdfs.cp, fn, fn_copy_on_wd)
    fn_copy_on_dest_dir = "%s/fn" % dest_dir
    hdfs.cp(fn, dest_dir)
    self.assertEqual(hdfs.load(fn_copy_on_dest_dir), self.data)
    self.assertRaises(IOError, hdfs.cp, fn, dest_dir)

  def __cp_dir(self, wd):
    src_dir = "%s/src_dir" % wd
    hdfs.mkdir(src_dir)
    copy_on_wd = "%s/src_dir_copy" % wd
    copy_on_copy_on_wd = "%s/src_dir" % copy_on_wd
    hdfs.cp(src_dir, copy_on_wd)
    self.assertTrue(hdfs.path.exists(copy_on_wd))
    hdfs.cp(src_dir, copy_on_wd)
    self.assertTrue(hdfs.path.exists(copy_on_copy_on_wd))
    self.assertRaises(IOError, hdfs.cp, src_dir, copy_on_wd)

  def __cp_recursive(self, wd):
    src_t = self.__make_tree(wd)
    src = src_t.name
    copy_on_wd = "%s_copy" % src
    src_bn, copy_on_wd_bn = [hdfs.path.basename(d) for d in (src, copy_on_wd)]
    hdfs.cp(src, copy_on_wd)
    for t in src_t.walk():
      copy_name = t.name.replace(src_bn, copy_on_wd_bn)
      self.assertTrue(hdfs.path.exists(copy_name))
      if t.kind == 0:
        self.assertEqual(hdfs.load(copy_name), self.data)
    hdfs.cp(src, copy_on_wd)
    for t in src_t.walk():
      copy_name = t.name.replace(src_bn, "%s/%s" % (copy_on_wd_bn, src_bn))
      self.assertTrue(hdfs.path.exists(copy_name))
      if t.kind == 0:
        self.assertEqual(hdfs.load(copy_name), self.data)

  def cp(self):
    print
    for wd in self.local_wd, self.hdfs_wd:
      print "  on %s ..." % wd
      print "    file ..."
      self.__cp_file(wd)
      print "    dir ..."
      self.__cp_dir(wd)
      print "    recursive ..."
      self.__cp_recursive(wd)

  def put(self):
    src = hdfs.path.split(self.local_paths[0])[-1]
    dest = self.hdfs_paths[0]
    with open(src, "w") as f:
      f.write(self.data)
    hdfs.put(src, dest)
    with hdfs.open(dest) as fi:
      rdata = fi.read()
    self.assertEqual(rdata, self.data)

  def get(self):
    src = self.hdfs_paths[0]
    dest = hdfs.path.split(self.local_paths[0])[-1]
    hdfs.dump(self.data, src)
    hdfs.get(src, dest)
    with open(dest) as fi:
      rdata = fi.read()
    self.assertEqual(rdata, self.data)

  def rmr(self):
    for wd in self.local_wd, self.hdfs_wd:
      t1 = self.__make_tree(wd)
      hdfs.rmr(t1.name)
      self.assertEqual(len(hdfs.ls(wd)), 0)

  def chmod(self):
    with tempfile.NamedTemporaryFile() as f:
      hdfs.chmod("file://" + f.name, 444)
      s = os.stat(f.name)
      self.assertEqual(444, stat.S_IMODE(s.st_mode))

  def move(self):
    for wd in self.local_wd, self.hdfs_wd:
      t1 = self.__make_tree(wd)
      t2 = [_ for _ in t1.children if _.kind == 1][0]
      f2 = t2.children[0]
      hdfs.move(f2.name, t1.name)
      ls = [os.path.basename(_) for _ in hdfs.ls(t1.name)]
      self.assertTrue(os.path.basename(f2.name) in ls)
      self.assertEqual(len(hdfs.ls(t2.name)), 0)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestHDFS("open"))
  suite.addTest(TestHDFS("dump"))
  suite.addTest(TestHDFS("lsl"))
  suite.addTest(TestHDFS("ls"))
  suite.addTest(TestHDFS("mkdir"))
  suite.addTest(TestHDFS("load"))
  suite.addTest(TestHDFS("cp"))
  suite.addTest(TestHDFS("put"))
  suite.addTest(TestHDFS("get"))
  suite.addTest(TestHDFS("rmr"))
  suite.addTest(TestHDFS("chmod"))
  suite.addTest(TestHDFS("move"))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
