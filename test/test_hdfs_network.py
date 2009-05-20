from test_hdfs_basic_class import hdfs_basic_tc, HDFS

import unittest, os

class hdfs_default_tc(hdfs_basic_tc):
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, 'default', 0)

  def connect_disconnect(self):
    fs = HDFS(self.HDFS_HOST, self.HDFS_PORT)
    blk_size = fs.default_block_size()
    capacity = fs.capacity()
    used     = fs.used()
    fs.close()
    print 'blk_size = ', blk_size
    print 'capacity = ', capacity
    print 'used     = ', used
  def copy(self):
    fs = HDFS(self.HDFS_HOST, self.HDFS_PORT)
    fs_plain_disk = HDFS('', 0)
    path = 'foobar.txt'
    txt  = 'hello there!'
    N  = 10
    data = self._write_example_file(path, N, txt, fs_plain_disk)
    fs_plain_disk.copy(path, fs, path)
    fs_plain_disk.delete(path)
    self.assertFalse(fs_plain_disk.exists(path))
    self.assertTrue(fs.exists(path))
    flags = os.O_RDONLY
    f = fs.open_file(path, flags, 0, 0, 0)
    data2 = f.read(len(data))
    self.assertEqual(len(data2), len(data),
                     "wrong number of bytes read.")
    self.assertEqual(data2, data,
                     "wrong bytes read.")
    f.close()
    fs.delete(path)
    fs.close()
  #--
  def move(self):
    fs = HDFS(self.HDFS_HOST, self.HDFS_PORT)
    fs_plain_disk = HDFS('', 0)
    path = 'foobar.txt'
    txt  = 'hello there!'
    N  = 10
    data = self._write_example_file(path, N, txt, fs_plain_disk)
    fs_plain_disk.move(path, fs, path)
    self.assertFalse(fs_plain_disk.exists(path))
    self.assertTrue(fs.exists(path))
    flags = os.O_RDONLY
    f = fs.open_file(path, flags, 0, 0, 0)
    data2 = f.read(len(data))
    self.assertEqual(len(data2), len(data),
                     "wrong number of bytes read.")
    self.assertEqual(data2, data,
                     "wrong bytes read.")
    f.close()
    fs.delete(path)
    fs.close()

#----------------------------------------------------------------------------
class hdfs_local_tc(hdfs_default_tc):
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, 'localhost', 9000)

#----------------------------------------------------------------------------
def suite():
  suite = unittest.TestSuite()
  #--
  suite.addTest(hdfs_default_tc('connect_disconnect'))
  suite.addTest(hdfs_default_tc('open_close'))
  suite.addTest(hdfs_default_tc('write_read'))
  suite.addTest(hdfs_default_tc('rename'))
  suite.addTest(hdfs_default_tc('change_dir'))
  suite.addTest(hdfs_default_tc('create_dir'))
  suite.addTest(hdfs_default_tc('copy'))
  suite.addTest(hdfs_default_tc('move'))
  suite.addTest(hdfs_default_tc('available'))
  suite.addTest(hdfs_default_tc('list_directory'))
  #--
  suite.addTest(hdfs_local_tc('connect_disconnect'))
  suite.addTest(hdfs_local_tc('open_close'))
  suite.addTest(hdfs_local_tc('write_read'))
  suite.addTest(hdfs_local_tc('rename'))
  suite.addTest(hdfs_local_tc('change_dir'))
  suite.addTest(hdfs_local_tc('create_dir'))
  suite.addTest(hdfs_local_tc('copy'))
  suite.addTest(hdfs_local_tc('move'))
  suite.addTest(hdfs_local_tc('available'))
  suite.addTest(hdfs_local_tc('list_directory'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

