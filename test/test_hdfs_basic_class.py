# BEGIN_COPYRIGHT
# END_COPYRIGHT
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

  def chmod(self):
    path = "/tmp/pydoop_test_chmod"
    new_perm = 0777
    try:
      self.fs.delete(path)
    except IOError:
      pass
    self.fs.create_directory(path)
    old_perm = self.fs.get_path_info(path)["permissions"]
    self.fs.chmod(path, new_perm)
    self.assertEqual(self.fs.get_path_info(path)["permissions"], new_perm)
    self.fs.chmod(path, old_perm)
    self.assertEqual(self.fs.get_path_info(path)["permissions"], old_perm)
    self.fs.delete(path)

  def connect(self):
    pass
  
  def open_close(self):
    path = 'foobar.txt'
    flags = os.O_WRONLY
    buff_size = 0
    replication = 0
    blocksize = 0
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

  def flush(self):
    path = "/tmp/test_hdfs_flush"
    f = self.fs.open_file(path, os.O_WRONLY)
    f.write("foo")
    f.flush()
    f.close()

  def _write_example_file(self, path, N, txt, fs=None,
                          buffer_size=0, replication=0, block_size=0):
    if not fs:
      fs = self.fs
    flags = os.O_WRONLY
    f = fs.open_file(path, flags, buffer_size, replication, block_size)
    data = ''
    txt = 'hello there!'
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
    txt = 'hello there!'
    N = 10
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
    txt = 'hello there!'
    N = 10
    data = self._write_example_file(path, N, txt)
    info = self.fs.get_path_info(path)
    self.__check_path_info(info, kind="file", size=N*len(txt))
    self.assertEqual(info['name'].rsplit("/",1)[1], path)
    self.fs.delete(path)
    # test on dir path
    path = "test_get_path_info"
    self.fs.create_directory(path)
    info = self.fs.get_path_info(path)
    self.__check_path_info(info, kind="directory")
    self.assertEqual(info['name'].rsplit("/",1)[1], path)
    self.fs.delete(path)
    self.assertRaises(IOError, self.fs.get_path_info, path)

  def write_read(self):
    fs = HDFS(self.HDFS_HOST, self.HDFS_PORT)
    path = 'foobar.txt'
    txt = 'hello there!'
    N = 10
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
    self.fs.delete(path)

  def copy(self):
    pass

  def copy_on_self(self):
    fs = HDFS(self.HDFS_HOST, self.HDFS_PORT)
    path = 'foobar.txt'
    path1 = 'foobar1.txt'
    txt  = 'hello there!'
    N  = 10
    data = self._write_example_file(path, N, txt, fs)
    fs.copy(path, fs, path1)
    fs.delete(path)
    self.assertFalse(fs.exists(path))
    self.assertTrue(fs.exists(path1))
    flags = os.O_RDONLY
    f = fs.open_file(path1, flags, 0, 0, 0)
    data2 = f.read(len(data))
    self.assertEqual(len(data2), len(data),
                     "wrong number of bytes read.")
    self.assertEqual(data2, data,
                     "wrong bytes read.")
    f.close()
    fs.delete(path1)
    fs.close()

  def move(self):
    pass

  def rename(self):
    old_path = 'foobar.txt'
    new_path = 'MOVED-' + old_path
    txt = 'hello there!'
    N = 100
    data = self._write_example_file(old_path, N, txt)
    self.fs.rename(old_path, new_path)
    self.assertTrue(self.fs.exists(new_path))
    self.assertFalse(self.fs.exists(old_path))
    self.fs.delete(new_path)

  def change_dir(self):
    cwd = self.fs.working_directory()
    new_d = os.path.join(cwd, 'foo/bar/dir')  # does not need to exist
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
    self.assertEqual(self.fs.list_directory(new_d), [])
    basenames = 'bar.txt', 'foo.txt'
    paths = [os.path.join(new_d, fn) for fn in basenames]
    txt = 'hello there!'
    N = 10
    data = [self._write_example_file(p, N, txt) for p in paths]
    infos = self.fs.list_directory(new_d)
    self.assertEqual(len(infos), len(paths))
    for i in infos:
      self.__check_path_info(i, kind="file", size=N*len(txt))
      self.assertTrue(i['name'].rsplit("/",1)[1] in basenames)
    for p in paths:
      self.fs.delete(p)
    self.fs.delete(os.path.join(cwd, parts[0]))
    self.assertRaises(IOError, self.fs.list_directory, new_d)

  def readline(self):
    samples = [
      "foo\nbar\n\ntar",
      "\nfoo\nbar\n\ntar",
      "foo\nbar\n\ntar\n",
      "\n\n\n", "\n", "",
      "foobartar",
      ]
    path = "foobar.txt"
    for text in samples:
      expected_lines = text.splitlines(True)
      for chunk_size in 2, max(1, len(text)), 2+len(text):
        lines = []
        f = self.fs.open_file(path, os.O_WRONLY, 0, 0, 0, chunk_size)
        f.write(text)
        f.close()
        f = self.fs.open_file(path, os.O_RDONLY, 0, 0, 0, chunk_size)
        while 1:
          l = f.readline()
          if l == "":
            break
          lines.append(l)
        f.close()
        self.assertEqual(lines, expected_lines)
    self.fs.delete(path)

  def seek(self):
    lines = ["1\n", "2\n", "3\n"]
    text = "".join(lines)
    path = "foobar.txt"
    for chunk_size in range(1, 2+len(text)):
      f = self.fs.open_file(path, os.O_WRONLY, 0, 0, 0, chunk_size)
      f.write(text)
      f.close()
      f = self.fs.open_file(path, os.O_RDONLY, 0, 0, 0, chunk_size)
      for i, l in enumerate(lines):
        f.seek(sum(map(len, lines[:i])))
        self.assertEqual(f.readline(), lines[i])
        f.seek(0)
        self.assertEqual(f.readline(), lines[0])
        f.seek(sum(map(len, lines[:i])))
        self.assertEqual(f.readline(), lines[i])
      f.close()
    self.fs.delete(path)

  def __check_path_info(self, info, **expected_values):
    keys = ('kind', 'group', 'name', 'last_mod', 'replication', 'owner',
            'permissions', 'block_size', 'last_access', 'size')
    for k in keys:
      self.assertTrue(k in info)
    for k, exp_v in expected_values.iteritems():
      v = info[k]
      self.assertEqual(v, exp_v)


def basic_tests():
  return [
    'chmod',
    'connect',
    'open_close',
    'flush',
    'write_read',
    'write_read_chunk',
    'rename',
    'change_dir',
    'create_dir',
    'copy_on_self',
    'available',
    'get_path_info',
    'list_directory',
    'readline',
    'seek'
    ]
