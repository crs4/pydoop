# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, getpass
from test_hdfs_basic_class import hdfs_basic_tc, basic_tests
import pydoop.hdfs as hdfs


class TestConnection(unittest.TestCase):

  def runTest(self):
    current_user = getpass.getuser()
    for user in None, current_user, "nobody":
      expected_user = current_user
      fs = hdfs.hdfs("", 0, user=user)
      self.assertEqual(fs.user, expected_user)
      fs.close()


class hdfs_plain_disk_tc(hdfs_basic_tc):
  
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, '', 0)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestConnection('runTest'))
  tests = basic_tests()
  for t in tests:
    suite.addTest(hdfs_plain_disk_tc(t))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
