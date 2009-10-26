import sys, os, unittest
from ctypes import create_string_buffer
from pydoop.hdfs import hdfs as HDFS


class hdfs_basic_tc(unittest.TestCase):
  
  def __init__(self, target, HDFS_HOST='', HDFS_PORT=0):
    unittest.TestCase.__init__(self, target)
    self.HDFS_HOST = HDFS_HOST
    self.HDFS_PORT = HDFS_PORT
    
  def setUp(self):
    self.fs = HDFS(self.HDFS_HOST, self.HDFS_PORT)
    
  def tearDown(self):
    self.fs.close()
    
  def failUnlessRaisesExternal(self, excClass, callableObj, *args, **kwargs):
    sys.stderr.write(
      "\n--- TESTING EXTERNAL EXCEPTION, ERROR MESSAGES ARE EXPECTED ---\n")
    self.failUnlessRaises(excClass, callableObj, *args, **kwargs)
    sys.stderr.write(
      "--- DONE TESTING EXTERNAL EXCEPTION ---------------------------\n")
    
  assertRaisesExternal = failUnlessRaisesExternal
  
  def connect_disconnect(self):
    blk_size = self.fs.default_block_size()
    capacity = 0 #fs.capacity()
    used     = 0 #fs.used()
    
  def open_close(self):
    path = 'foobar.txt'
    flags = os.O_WRONLY
    buff_size   = 0
    replication = 0
    blocksize   = 0
    f = self.fs.open_file(path, flags, buff_size, replication, blocksize)
    f.close()
    self.assertTrue(self.fs.exists(path))
    flags = os.O_RDONLY
    f = self.fs.open_file(path, flags, buff_size, replication, blocksize)
    f.close()
    self.fs.delete(path)
    self.assertRaisesExternal(
      IOError,
      self.fs.open_file, path, flags, buff_size, replication, blocksize
      )
    
  def _write_example_file(self, path, N, txt, fs=None,
                          buffer_size=0, replication=0, block_size=0):
    if not fs:
      fs = self.fs
    flags = os.O_WRONLY
    f = fs.open_file(path, flags, buffer_size, replication, block_size)
    data = ''
    txt  = 'hello there!'
    for i in range(N):
      res = f.write(txt)
      data += txt
      self.assertEqual(res, len(txt),
                       "wrong number of bytes written.")
    f.close()
    return data
  
  def available(self):
    fs = HDFS(self.HDFS_HOST, self.HDFS_PORT)
    path = 'foobar.txt'
    txt  = 'hello there!'
    N  = 10
    data = self._write_example_file(path, N, txt)
    #--
    flags = os.O_RDONLY
    f = self.fs.open_file(path, flags, 0, 0, 0)
    self.assertEqual(len(data), f.available())
    f.close()
    self.fs.delete(path)
    
  def get_path_info(self):
    fs = HDFS(self.HDFS_HOST, self.HDFS_PORT)
    path = 'foobar.txt'
    txt  = 'hello there!'
    N  = 10
    data = self._write_example_file(path, N, txt)
    #--
    info = self.fs.get_path_info(path)
    for k in ('kind group name last_mod replication owner'
              + ' permissions block_size last_access size').split():
      self.assertTrue(info.has_key(k))
    self.assertEqual(info['kind'], 'file')
    self.assertEqual(info['size'], 120)
    #self.assertEqual(info['permissions'], 420)
    print '\nPATH INFO =', info
    self.fs.delete(path)
    
  def write_read(self):
    fs = HDFS(self.HDFS_HOST, self.HDFS_PORT)
    path = 'foobar.txt'
    txt  = 'hello there!'
    N  = 10
    data = self._write_example_file(path, N, txt)
    #--
    flags = os.O_RDONLY
    f = self.fs.open_file(path, flags, 0, 0, 0)
    data2 = f.read(len(data))
    self.assertEqual(len(data2), len(data), "wrong number of bytes read.")
    self.assertEqual(data2, data, "wrong bytes read.")
    f.close()
    #--
    f = self.fs.open_file(path, flags, 0, 0, 0)
    pos = 0
    for i in range(N):
      txt2 = f.read(len(txt))
      self.assertEqual(len(txt2), len(txt), "wrong number of bytes read.")
      self.assertEqual(txt2, txt, "wrong bytes read.")
      pos += len(txt)
      self.assertEqual(pos, f.tell())
    f.close()
    #--
    f = self.fs.open_file(path, flags, 0, 0, 0)
    pos = 0
    for i in range(N):
      txt2 = f.pread(pos, len(txt))
      self.assertEqual(len(txt2), len(txt), "wrong number of bytes pread.")
      self.assertEqual(txt2, txt, "wrong pread.")
      self.assertEqual(0, f.tell())
      pos += len(txt)
    f.close()
    flags = os.O_RDONLY
    f = fs.open_file(path, flags, 0, 0, 0)
    self.assertRaisesExternal(IOError, f.write, txt)
    f.close()
    #--
    self.fs.delete(path)
    
  def write_read_chunk(self):
    fs = HDFS(self.HDFS_HOST, self.HDFS_PORT)
    path = 'foobar.txt'
    txt  = 'hello there!'
    N  = 10
    data = self._write_example_file(path, N, txt)
    #--
    flags = os.O_RDONLY
    f = self.fs.open_file(path, flags, 0, 0, 0)
    chunk = create_string_buffer(len(data))
    bytes_read = f.read_chunk(chunk)
    self.assertEqual(bytes_read, len(data), "wrong number of bytes read.")
    for i in range(len(data)):
      self.assertEqual(chunk[i], data[i], "wrong bytes read at %d:>%s< >%s<" %
                       (i, chunk[i], data[i]))
    f.close()
    #--
    f = self.fs.open_file(path, flags, 0, 0, 0)
    pos = 0
    chunk = create_string_buffer(len(txt))
    for i in range(N):
      bytes_read = f.pread_chunk(pos, chunk)
      self.assertEqual(bytes_read, len(txt), "wrong number of bytes read.")
      for c in range(len(txt)):
        self.assertEqual(chunk[c], txt[c], "wrong bytes read at %d." % c)
      pos += len(txt)
      # It is unclear if this is a bug or a feature of the API.  I
      # guess the problem is that there is not an fseek function, and
      # thus when one uses a pread it basically does a random access.
      self.assertEqual(0, f.tell())
    f.close()
    
  def copy(self):
    pass
  
  def move(self):
    pass
  
  def rename(self):
    old_path = 'foobar.txt'
    new_path = 'MOVED-' + old_path
    txt  = 'hello there!'
    N  = 100
    data = self._write_example_file( old_path, N, txt)
    self.fs.rename(old_path, new_path)
    self.assertTrue(self.fs.exists(new_path))
    self.assertFalse(self.fs.exists(old_path))
    self.fs.delete(new_path)
    
  def change_dir(self):
    cwd = self.fs.working_directory()
    new_d = os.path.join(cwd, 'foo/bar/dir')
    self.fs.set_working_directory(new_d)
    self.assertEqual(self.fs.working_directory(), new_d)
    self.fs.set_working_directory(cwd)
    self.assertEqual(self.fs.working_directory(), cwd)
    
  def create_dir(self):
    cwd = self.fs.working_directory()
    parts = ['foo', 'bar', 'dir']
    new_d = os.path.join(cwd, '/'.join(parts))
    self.fs.create_directory(new_d)
    p = cwd
    ps = []
    for x in parts:
      p = os.path.join(p, x)
      ps.insert(0, p)
    for x in ps:
      self.assertTrue(self.fs.exists(x))
      self.fs.delete(x)
      self.assertFalse(self.fs.exists(x))
      
  def list_directory(self):
    cwd = self.fs.working_directory()
    parts = ['foo', 'bar', 'dir']
    new_d = os.path.join(cwd, '/'.join(parts))
    self.fs.create_directory(new_d)
    self.assertTrue(self.fs.exists(new_d))
    path = os.path.join(new_d, 'foobar.txt')
    txt  = 'hello there!'
    N  = 10
    data = self._write_example_file( path, N, txt)
    print "\nDIR LIST =", self.fs.list_directory(new_d)
    self.fs.delete(path)
    self.fs.delete(new_d)


def basic_tests():
  return [
    'connect_disconnect',
    'open_close',
    'write_read',
    'write_read_chunk',
    'rename',
    'change_dir',
    'create_dir',
    'available',
    'get_path_info',
    'list_directory'
    ]
