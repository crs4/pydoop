# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, tempfile, os, pwd, grp
from test_hdfs_basic_class import hdfs_basic_tc, basic_tests
from pydoop.hdfs import hdfs as HDFS


class hdfs_plain_disk_tc(hdfs_basic_tc):
  
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, '', 0)
  
  def connect(self):
    fs_list = [
      HDFS(self.HDFS_HOST, self.HDFS_PORT),
      HDFS(self.HDFS_HOST, self.HDFS_PORT, "nobody"),
      HDFS(self.HDFS_HOST, self.HDFS_PORT, "nobody", ["users"]),
      HDFS(self.HDFS_HOST, self.HDFS_PORT, "nobody", ["users", "nobody"]),
      ]
    for fs in fs_list:
      self.__connect_helper(fs)
      fs.close()

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


def suite():
  suite = unittest.TestSuite()
  tests = basic_tests()
  for t in tests:
    suite.addTest(hdfs_plain_disk_tc(t))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
