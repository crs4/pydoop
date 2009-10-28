# BEGIN_COPYRIGHT
# END_COPYRIGHT
import unittest, os
from test_hdfs_basic_class import hdfs_basic_tc, basic_tests, HDFS


class hdfs_default_tc(hdfs_basic_tc):
  
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, 'default', 0)

  def connect_disconnect(self):
    fs = HDFS(self.HDFS_HOST, self.HDFS_PORT)
    blk_size = fs.default_block_size()
    capacity = fs.capacity()
    used     = fs.used()
    fs.close()
    print '\nblk_size = ', blk_size
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
    
  def block_size(self):
    txt = "hello there!"
    for bs_MB in xrange(100, 500, 50):
      bs = bs_MB * 2**20
      path = "test_bs_%d.txt" % bs_MB
      f = self.fs.open_file(path, os.O_WRONLY, 0, 0, bs)
      _ = f.write(txt)
      f.close()
      info = self.fs.get_path_info(path)
      try:
        actual_bs = info["block_size"]
      except KeyError:
        sys.stderr.write(
          "No info on block size! Check the 'get_path_info' test result")
        break
      else:
        self.assertEqual(bs, actual_bs)

  def replication(self):
    txt = "hello there!"
    for r in xrange(1, 6):
      path = "test_replication_%d.txt" % r
      f = self.fs.open_file(path, os.O_WRONLY, 0, r, 0)
      _ = f.write(txt)
      f.close()
      info = self.fs.get_path_info(path)
      try:
        actual_r = info["replication"]
      except KeyError:
        sys.stderr.write(
          "No info on replication! Check the 'get_path_info' test result")
        break
      else:
        self.assertEqual(r, actual_r)


class hdfs_local_tc(hdfs_default_tc):
  
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, 'localhost', 9000)


def suite():
  suite = unittest.TestSuite()
  tests = basic_tests()
  tests.extend(['copy', 'move', 'block_size', 'replication'])
  for tc in hdfs_default_tc, hdfs_local_tc:
    for t in tests:
      suite.addTest(tc(t))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
