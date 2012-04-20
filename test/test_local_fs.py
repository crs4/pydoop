# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, getpass

import pydoop.hdfs as hdfs
from common_hdfs_tests import TestCommon, common_tests


class TestConnection(unittest.TestCase):

  def runTest(self):
    current_user = getpass.getuser()
    for user in None, current_user, "nobody":
      expected_user = current_user
      fs = hdfs.hdfs("", 0, user=user)
      self.assertEqual(fs.user, expected_user)
      fs.close()


class TestLocalFS(TestCommon):
  
  def __init__(self, target):
    TestCommon.__init__(self, target, '', 0)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestConnection('runTest'))
  tests = common_tests()
  for t in tests:
    suite.addTest(TestLocalFS(t))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
