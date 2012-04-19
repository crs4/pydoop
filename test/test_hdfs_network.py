# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, os, uuid, shutil, getpass

from test_hdfs_basic_class import hdfs_basic_tc, basic_tests, make_wd
from utils import HDFS_HOST, HDFS_PORT
import pydoop.hdfs as hdfs


class TestConnection(unittest.TestCase):

  def runTest(self):
    for host, port in (("default", 0), (HDFS_HOST, HDFS_PORT)):
      current_user = getpass.getuser()
      for user in None, current_user, "nobody":
        expected_user = user or current_user
        fs = hdfs.hdfs(host, port, user=user)
        self.assertEqual(fs.user, expected_user)
        fs.close()


class hdfs_default_tc(hdfs_basic_tc):
  
  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, 'default', 0)

  def capacity(self):
    c = self.fs.capacity()
    self.assertTrue(isinstance(c, (int,long)))

  def default_block_size(self):
    dbs = self.fs.default_block_size()
    self.assertTrue(isinstance(dbs, (int,long)))

  def used(self):
    u = self.fs.used()
    self.assertTrue(isinstance(u, (int,long)))

  def chown(self):
    new_owner = "nobody"
    new_group = "users"
    path = self._make_random_file()
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

  def utime(self):
    path = self._make_random_file()
    old_mtime = self.fs.get_path_info(path)["last_mod"]
    old_atime = self.fs.get_path_info(path)["last_access"]
    new_mtime = old_mtime - 500
    new_atime = old_mtime - 100
    self.fs.utime(path, new_mtime, new_atime)
    self.assertEqual(self.fs.get_path_info(path)["last_mod"], int(new_mtime))
    self.assertEqual(self.fs.get_path_info(path)["last_access"], int(new_atime))
    self.fs.utime(path, old_mtime, old_atime)
    self.assertEqual(self.fs.get_path_info(path)["last_mod"], int(old_mtime))
    self.assertEqual(self.fs.get_path_info(path)["last_access"], int(old_atime))

  def copy(self):
    local_fs = hdfs.hdfs('', 0)
    local_wd = make_wd(local_fs)
    from_path = os.path.join(local_wd, uuid.uuid4().hex)
    content = uuid.uuid4().hex
    with open(from_path, "w") as f:
      f.write(content)
    to_path = self._make_random_file()
    local_fs.copy(from_path, self.fs, to_path)
    local_fs.close()
    with self.fs.open_file(to_path) as f:
      self.assertEqual(f.read(), content)
    shutil.rmtree(local_wd)

  def move(self):
    content = uuid.uuid4().hex
    from_path = self._make_random_file(content=content)
    to_path = self._make_random_path()
    self.fs.move(from_path, self.fs, to_path)
    self.assertFalse(self.fs.exists(from_path))
    with self.fs.open_file(to_path) as f:
      self.assertEqual(f.read(), content)

  def block_size(self):
    for bs_MB in xrange(100, 500, 50):
      bs = bs_MB * 2**20
      path = self._make_random_file(blocksize=bs)
      self.assertEqual(self.fs.get_path_info(path)["block_size"], bs)

  def replication(self):
    for r in xrange(1, 6):
      path = self._make_random_file(replication=r)
      self.assertEqual(self.fs.get_path_info(path)["replication"], r)

  def set_replication(self):
    old_r, new_r = 2, 3
    path = self._make_random_file(replication=old_r)
    self.fs.set_replication(path, new_r)
    self.assertEqual(self.fs.get_path_info(path)["replication"], new_r)

  # HDFS returns less than the number of requested bytes if the chunk
  # being read crosses the boundary between data blocks.
  def readline_block_boundary(self):
    bs = 512  # FIXME: hardwired to the default value of io.bytes.per.checksum
    line = "012345678\n"
    path = self._make_random_path()
    with self.fs.open_file(path, flags="w", blocksize=bs) as f:
      bytes_written = lines_written = 0
      while bytes_written < bs + 1:
        f.write(line)
        lines_written += 1
        bytes_written += len(line)
    with self.fs.open_file(path) as f:
      lines = []
      while 1:
        l = f.readline()
        if l == "":
          break
        lines.append(l)
    self.assertEqual(len(lines), lines_written)
    for i, l in enumerate(lines):
      self.assertEqual(l, line, "line %d: %r != %r" % (i, l, line))

  def readline_big(self):
    for i in xrange(10, 23):
      x = '*' * (2**i) + "\n"
      path = self._make_random_file(content=x)
      with self.fs.open_file(path) as f:
        l = f.readline()
      self.assertEqual(l, x, "len(a) = %d, len(x) = %d" % (len(l), len(x)))

  def get_hosts(self):
    blocksize = 4096
    N = 4
    content = "x" * blocksize * N
    path = self._make_random_file(content=content, blocksize=blocksize)
    start = 0
    for i in xrange(N):
      length = blocksize * i + 1
      hosts_per_block = self.fs.get_hosts(path, start, length)
      self.assertEqual(len(hosts_per_block), i+1)


class hdfs_local_tc(hdfs_default_tc):

  def __init__(self, target):
    hdfs_basic_tc.__init__(self, target, HDFS_HOST, HDFS_PORT)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestConnection('runTest'))
  tests = basic_tests()
  tests.extend([
    'capacity',
    'default_block_size',
    'used',
    'chown',
    'utime',
    'copy',
    'move',
    'block_size',
    'replication',
    'set_replication',
    'readline_block_boundary',
    'readline_big',
    'get_hosts'
    ])
  for tc in hdfs_default_tc, hdfs_local_tc:
    for t in tests:
      suite.addTest(tc(t))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
