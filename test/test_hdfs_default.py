import unittest
import random

import sys
import os
import numpy as np

#----------------------------------------------------------------------------
from pydoop_core import hdfs_fs as HDFS
#----------------------------------------------------------------------------

HDFS_HOST='default'
HDFS_PORT=0

class hdfs_local_tc(unittest.TestCase):
  def connect_disconnect(self):
    fs = HDFS(HDFS_HOST, HDFS_PORT)
    blk_size = fs.default_block_size()
    #self.assertEqual(blk_size,  0)
    capacity = 0 #fs.capacity()
    used     = 0 #fs.used()
    fs.close()
    print 'blk_size = ', blk_size
    print 'capacity = ', capacity
    print 'used     = ', used
  #--
  def open_close(self):
    path = 'foobar.txt'
    fs = HDFS(HDFS_HOST, HDFS_PORT)
    flags = os.O_WRONLY
    buff_size   = 0
    replication = 0
    blocksize   = 0
    f = fs.open_file(path, flags, buff_size, replication, blocksize)
    f.close()
    self.assertTrue(fs.exists(path))
    fs.delete(path)
    fs.close()
  #--
  def _write_read_helper(self, fs, path, N, txt):
    flags = os.O_WRONLY
    f = fs.open_file(path, flags, 0, 0, 0)
    data = ''
    txt  = 'hello there!'
    for i in range(N):
      res = f.write(txt)
      data += txt
      self.assertEqual(res, len(txt),
                       "wrong number of bytes written.")
    f.close()
    return data
  #--
  def write_read(self):
    fs = HDFS(HDFS_HOST, HDFS_PORT)
    path = 'foobar.txt'
    txt  = 'hello there!'
    N  = 10
    data = self._write_read_helper(fs, path, N, txt)
    #--
    flags = os.O_RDONLY
    f = fs.open_file(path, flags, 0, 0, 0)
    data2 = f.read(len(data))
    self.assertEqual(len(data2), len(data),
                     "wrong number of bytes read.")
    self.assertEqual(data2, data,
                     "wrong bytes read.")
    f.close()
    #--
    f = fs.open_file(path, flags, 0, 0, 0)
    pos = 0
    for i in range(N):
      txt2 = f.pread(pos, len(txt))
      self.assertEqual(len(txt2), len(txt),
                       "wrong number of bytes pread.")
      self.assertEqual(txt2, txt,
                       "wrong pread.")
      pos += len(txt)
      self.assertEqual(pos, f.tell())
    f.close()
    #--
    fs.delete(path)
    fs.close()
    #--
  #--
  def write_read_chunk(self):
    fs = HDFS(HDFS_HOST, HDFS_PORT)
    path = 'foobar.txt'
    txt  = 'hello there!'
    N  = 10
    data = self._write_read_helper(fs, path, N, txt)
    #--
    flags = os.O_RONLY
    f = fs.open_file(path, flags, 0, 0, 0)
    chunk = np.zeros((len(data),), np.Char)
    bytes_read = f.read_chunk(chunk)
    self.assertEqual(bytes_read, len(data),
                     "wrong number of bytes read.")
    for i in range(len(data)):
      self.assertEqual(chunk[i], data[i],
                     "wrong bytes read at %d." % i)
    f.close()
    #--
    f = fs.open_file(path, flags, 0, 0, 0)
    pos = 0
    chunk = np.zeros((len(txt),), np.Char)
    for i in range(N):
      bytes_read = f.pread(pos, chunk)
      self.assertEqual(bytes_read, len(txt),
                       "wrong number of bytes read.")
      for c in range(len(txt)):
        self.assertEqual(chunk[c], txt[c],
                         "wrong bytes read at %d." % c)
      pos += len(txt)
      self.assertEqual(pos, f.tell())
    f.close()
    #--
    fs.close()
    #--
  def copy(self):
    fs = HDFS(HDFS_HOST, HDFS_PORT)
    pass
    fs.close()
  #--
  def move(self):
    fs = HDFS(HDFS_HOST, HDFS_PORT)
    pass
    fs.close()
  #--
  def rename(self):
    fs = HDFS(HDFS_HOST, HDFS_PORT)
    old_path = 'foobar.txt'
    new_path = 'MOVED-' + old_path
    txt  = 'hello there!'
    N  = 100
    data = self._write_read_helper(fs, old_path, N, txt)
    fs.rename(old_path, new_path)
    self.assertTrue(fs.exists(new_path))
    self.assertFalse(fs.exists(old_path))
    fs.delete(new_path)
    fs.close()
  #--
  def change_dir(self):
    fs = HDFS(HDFS_HOST, HDFS_PORT)
    cwd = fs.working_directory()
    print 'CWD=', cwd
    new_d = os.path.join(cwd, 'foo/bar/dir')
    print 'NEW_D=', new_d
    fs.set_working_directory(new_d)
    self.assertEqual(fs.working_directory(), new_d)
    fs.set_working_directory(cwd)
    self.assertEqual(fs.working_directory(), cwd)
    fs.close()
  #--
  def create_dir(self):
    fs = HDFS(HDFS_HOST, HDFS_PORT)
    cwd = fs.working_directory()
    print 'CWD=', cwd
    parts = ['foo', 'bar', 'dir']
    new_d = os.path.join(cwd, '/'.join(parts))
    print 'NEW_D=', new_d
    fs.create_directory(new_d)
    p = cwd
    ps = []
    for x in parts:
      p = os.path.join(p, x)
      print 'p=', p
      ps.insert(0, p)
    for x in ps:
      print 'x=', x
      self.assertTrue(fs.exists(x))
      fs.delete(x)
      self.assertFalse(fs.exists(x))
    fs.close()


#----------------------------------------------------------------------------
def suite():
  suite = unittest.TestSuite()
  #--
  suite.addTest(hdfs_local_tc('connect_disconnect'))
  suite.addTest(hdfs_local_tc('open_close'))
  suite.addTest(hdfs_local_tc('write_read'))
  suite.addTest(hdfs_local_tc('rename'))
  suite.addTest(hdfs_local_tc('change_dir'))
  suite.addTest(hdfs_local_tc('create_dir'))
  #--
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

