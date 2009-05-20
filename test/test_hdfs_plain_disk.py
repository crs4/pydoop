from test_hdfs_basic_class import hdfs_basic_tc

import unittest

class hdfs_plain_disk_tc(hdfs_basic_tc):
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, '', 0)

#----------------------------------------------------------------------------
def suite():
  suite = unittest.TestSuite()
  #--
  suite.addTest(hdfs_plain_disk_tc('connect_disconnect'))
  suite.addTest(hdfs_plain_disk_tc('open_close'))
  suite.addTest(hdfs_plain_disk_tc('write_read'))
  suite.addTest(hdfs_plain_disk_tc('write_read_chunk'))
  suite.addTest(hdfs_plain_disk_tc('rename'))
  suite.addTest(hdfs_plain_disk_tc('change_dir'))
  suite.addTest(hdfs_plain_disk_tc('create_dir'))
  suite.addTest(hdfs_plain_disk_tc('available'))
  suite.addTest(hdfs_plain_disk_tc('list_directory'))
  #--
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

