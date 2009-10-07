import unittest
from test_hdfs_basic_class import hdfs_basic_tc, basic_tests


class hdfs_plain_disk_tc(hdfs_basic_tc):
  
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, '', 0)


def suite():
  suite = unittest.TestSuite()
  tests = basic_tests()
  for t in tests:
    suite.addTest(hdfs_plain_disk_tc(t))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
