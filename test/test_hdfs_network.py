# BEGIN_COPYRIGHT
# END_COPYRIGHT
import unittest, os, pwd
from test_hdfs_basic_class import hdfs_basic_tc, basic_tests, HDFS


class hdfs_default_tc(hdfs_basic_tc):
  
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, 'default', 0)

  def chown(self):
    path = "/tmp/pydoop_test_chown"
    new_owner = "nobody"
    new_group = "users"
    f = self.fs.open_file(path, os.O_WRONLY)
    f.write("foo")
    f.close()
    old_owner = self.fs.get_path_info(path)["owner"]
    old_group = self.fs.get_path_info(path)["group"]
    self.fs.chown(path, user=new_owner)
    self.assertEqual(self.fs.get_path_info(path)["owner"], new_owner)
    self.assertEqual(self.fs.get_path_info(path)["group"], old_group)
    self.fs.chown(path, group=new_group)
    self.assertEqual(self.fs.get_path_info(path)["owner"], new_owner)
    self.assertEqual(self.fs.get_path_info(path)["group"], new_group)
    self.fs.chown(path, old_owner, old_group)
    self.assertEqual(self.fs.get_path_info(path)["owner"], old_owner)
    self.assertEqual(self.fs.get_path_info(path)["group"], old_group)
    self.fs.delete(path)

  def connect(self):
    path = "/tmp/pydoop_test_connect"
    try:
      self.fs.delete(path)
    except IOError:
      pass
    self.fs.create_directory(path)
    default_user = self.fs.get_path_info(path)["owner"]
    default_group = self.fs.get_path_info(path)["group"]
    new_user = "nobody"
    new_group = "users"
    self.fs.chmod(path, 0777)
    print
    base_args = self.HDFS_HOST, self.HDFS_PORT
    cases = [ # (hdfs_args_tuple, (expected_owner, expected_group))
      (base_args, (default_user, default_group)),
      (base_args+(new_user,), (new_user, default_group)),
      
      # file group ownership does not change even if we pass a group
      # list. Maybe it's the way it's supposed to (not?) work: group
      # ownership is not checked in libhdfs/hdfs_test.c
      (base_args+(new_user, [new_group]), (new_user, default_group)),
      (base_args+(new_user, [new_group, new_user]), (new_user, default_group)),
      ]
    for hdfs_args, (expected_owner, expected_group) in cases:
      print "hdfs_args = %r" % (hdfs_args,)
      fs = HDFS(*hdfs_args)
      self.__connect_helper(fs, path, expected_owner, expected_group)
    self.fs.delete(path)
    
  def __connect_helper(self, fs, path, expected_owner, expected_group):
    file_path = "%s/%s_%s" % (path, expected_owner, expected_group)
    f = fs.open_file(file_path, os.O_WRONLY)
    f.write("foo")
    f.close()
    hdfs_path_info = fs.get_path_info(file_path)
    self.assertEqual(hdfs_path_info["owner"], expected_owner)
    self.assertEqual(hdfs_path_info["group"], expected_group)
    
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
      path = "foobar.txt"
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
      finally:
        self.fs.delete(path)

  def replication(self):
    txt = "hello there!"
    for r in xrange(1, 6):
      path = "foobar.txt"
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
      finally:
        self.fs.delete(path)      

  # HDFS returns less than the number of requested bytes if the chunk
  # being read crosses the boundary between data blocks.
  def readline_block_boundary(self):
    bs = 512  # FIXME: hardwired to the default value of io.bytes.per.checksum
    line = "012345678\n"
    path = "foobar.txt"
    f = self.fs.open_file(path, os.O_WRONLY, 0, 0, bs)
    bytes_written = lines_written = 0
    while bytes_written < bs + 1:
      f.write(line)
      lines_written += 1
      bytes_written += len(line)
    f.close()
    f = self.fs.open_file(path, os.O_RDONLY, 0, 0, bs)
    lines = []
    while 1:
      l = f.readline()
      if l == "":
        break
      lines.append(l)
    if f:
      f.close()
    self.assertEqual(len(lines), lines_written)
    for i, l in enumerate(lines):
      self.assertEqual(l, line, "line %d: %r != %r" % (i, l, line))
    self.fs.delete(path)

  def get_hosts(self):
    path = "test_get_hosts.txt"
    block_size = 4096
    N = 4
    text = "x" * block_size * N
    f = self.fs.open_file(path, os.O_WRONLY, 0, 0, block_size)
    f.write(text)
    f.close()
    start = 0
    for i in xrange(N):
      length = block_size * i + 1
      hosts_per_block = self.fs.get_hosts(path, start, length)
      self.assertEqual(len(hosts_per_block), i+1)
    self.fs.delete(path)


class hdfs_local_tc(hdfs_default_tc):
  
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, 'localhost', 9000)


def suite():
  suite = unittest.TestSuite()
  tests = basic_tests()
  tests.extend([
    'chown',
    'copy',
    'move',
    'block_size',
    'replication',
    'readline_block_boundary',
    'get_hosts'
    ])
  for tc in hdfs_default_tc, hdfs_local_tc:
    for t in tests:
      suite.addTest(tc(t))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
