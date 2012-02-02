# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, tempfile, os, pwd, grp
from test_hdfs_basic_class import hdfs_basic_tc, basic_tests
import pydoop.hdfs as hdfs


class hdfs_plain_disk_tc(hdfs_basic_tc):
  
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, '', 0)
  
  def connect(self):
    fs = hdfs.hdfs(self.HDFS_HOST, self.HDFS_PORT)
    self.__connection_assertions_and_close(fs)

  def connect_with_user(self):
    fs = hdfs.hdfs(self.HDFS_HOST, self.HDFS_PORT, "nobody")
    self.__connection_assertions_and_close(fs)

  def __connection_assertions_and_close(self, conn):
    self.assertFalse(conn.closed)
    self.__connect_helper(conn)
    self.assertEqual(conn.host, '')
    self.assertEqual(conn.port, 0)
    conn.close()
    self.assertTrue(conn.closed)
    self.assertRaises(ValueError, conn.default_block_size)

  def __connect_helper(self, fs):
    path = os.path.join(tempfile.mkdtemp(prefix="pydoop_test_hdfs"), "foo")
    f = fs.open_file(path, os.O_WRONLY)
    f.write("foo")
    f.close()
    hdfs_path_info = fs.get_path_info(path)
    hdfs_owner = hdfs_path_info["owner"]
    hdfs_group = hdfs_path_info["group"]
    local_path_info = os.stat(path)
    local_owner = pwd.getpwuid(local_path_info.st_uid).pw_name
    local_group = grp.getgrgid(local_path_info.st_gid).gr_name
    self.assertEqual(hdfs_owner, local_owner)
    self.assertEqual(hdfs_group, local_group)
    fs.delete(path)

  def top_level_open(self):
    path = "file:/tmp/test_hdfs_open"
    with hdfs.open(path, "w") as f:
      f.write(path)
    with hdfs.open(path) as f:
      self.assertEqual(f.read(), path)
    f.fs.delete(path)
    f.fs.close()


def suite():
  suite = unittest.TestSuite()
  tests = basic_tests()
  tests.append("connect_with_user")
  for t in tests:
    suite.addTest(hdfs_plain_disk_tc(t))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
