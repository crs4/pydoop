# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, unittest, random
from ctypes import create_string_buffer
import xml.dom.minidom

import pydoop.hdfs as hdfs
from pydoop.hadoop_utils import get_hadoop_version

HADOOP_HOME = hdfs.HADOOP_HOME
HADOOP_CONF_DIR = hdfs.HADOOP_CONF_DIR
HADOOP_VERSION = get_hadoop_version(HADOOP_HOME)


class hdfs_basic_tc(unittest.TestCase):

  DEFAULT_BYTES_PER_CHECKSUM = 512

  def __init__(self, target, HDFS_HOST='', HDFS_PORT=0):
    unittest.TestCase.__init__(self, target)
    self.HDFS_HOST = HDFS_HOST
    self.HDFS_PORT = HDFS_PORT

  def setUp(self):
    self.fs = hdfs.hdfs(self.HDFS_HOST, self.HDFS_PORT)

  def tearDown(self):
    self.fs.close()

  def failUnlessRaisesExternal(self, excClass, callableObj, *args, **kwargs):
    sys.stderr.write(
      "\n--- TESTING EXTERNAL EXCEPTION, ERROR MESSAGES ARE EXPECTED ---\n")
    self.failUnlessRaises(excClass, callableObj, *args, **kwargs)
    sys.stderr.write(
      "--- DONE TESTING EXTERNAL EXCEPTION ---------------------------\n")

  assertRaisesExternal = failUnlessRaisesExternal

  def delete(self):
    path = "/tmp/pydoop_test_delete/subdir"
    parent = path.rsplit("/", 1)[0]
    self.fs.create_directory(path)
    for i, s in enumerate(("foo", "bar")):
      fn = "%s/%s" % (path, s)
      f = self.fs.open_file(fn, os.O_WRONLY)
      f.write("%s\n" % s)
      f.close()
      self.fs.delete(fn, recursive=i)
      self.assertFalse(self.fs.exists(fn))
    f = self.fs.open_file("%s/tar" % (path), os.O_WRONLY)
    f.write("tar\n")
    f.close()
    if HADOOP_VERSION >= (0,21,0):
      self.assertRaisesExternal(IOError, self.fs.delete, path, recursive=False)
    self.fs.delete(path, recursive=True)
    self.assertFalse(self.fs.exists(path))
    self.fs.delete(parent, recursive=False)
    self.assertFalse(self.fs.exists(parent))
    
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
    for flags in "w", os.O_WRONLY:
      self.fs.open_file(path, flags).close()
      self.assertTrue(self.fs.exists(path))
    for flags in "r", os.O_RDONLY:
      self.fs.open_file(path, flags).close()
    #--
    f = self.fs.open_file(path, "r")
    self.assertFalse(f.closed)
    f.close()
    self.assertTrue(f.closed)
    self.assertRaises(ValueError, f.read, 1)
    #--
    self.assertRaises(ValueError, self.fs.open_file, path, "a")
    self.fs.delete(path)
    self.assertRaisesExternal(IOError, self.fs.open_file, path, "r")

  def file_attrs(self):
    path = "/tmp/test_file_attrs"
    f = self.fs.open_file(path, os.O_WRONLY)
    self.assertTrue(f.name.endswith(path))
    self.assertEqual(f.size, 0)
    self.assertEqual(f.mode, "w")
    f.write(path)
    f.close()
    self.assertEqual(f.size, len(path))
    f = self.fs.open_file(path)
    self.assertEqual(f.size, len(path))
    self.assertEqual(f.mode, "r")
    f.close()
    self.fs.delete(path)

  def flush(self):
    path = "/tmp/test_hdfs_flush"
    f = self.fs.open_file(path, os.O_WRONLY)
    f.write("foo")
    f.flush()
    f.close()
    self.fs.delete(path)

  def _write_example_file(self, path, N, txt, fs=None,
                          buffer_size=0, replication=0, block_size=0):
    if not fs:
      fs = self.fs
    flags = os.O_WRONLY
    f = fs.open_file(path, flags, buffer_size, replication, block_size)
    data = ''
    txt = 'hello there!'
    for _ in xrange(N):
      res = f.write(txt)
      data += txt
      self.assertEqual(res, len(txt), "wrong number of bytes written")
    f.close()
    return data

  def available(self):
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
    path = 'foobar.txt'
    txt = 'hello there!'
    N = 10
    _ = self._write_example_file(path, N, txt)
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
    for _ in xrange(N):
      txt2 = f.read(len(txt))
      self.assertEqual(len(txt2), len(txt), "wrong number of bytes read.")
      self.assertEqual(txt2, txt, "wrong bytes read.")
      pos += len(txt)
      self.assertEqual(pos, f.tell())
    f.close()
    #--
    f = self.fs.open_file(path, flags, 0, 0, 0)
    pos = 0
    for _ in xrange(N):
      txt2 = f.pread(pos, len(txt))
      self.assertEqual(len(txt2), len(txt), "wrong number of bytes pread.")
      self.assertEqual(txt2, txt, "wrong pread.")
      self.assertEqual(0, f.tell())
      pos += len(txt)
    f.close()
    flags = os.O_RDONLY
    f = self.fs.open_file(path, flags, 0, 0, 0)
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

  def read_all(self):
    path = "/tmp/test_read_all"
    blocksize = 32768
    L = 2 * blocksize
    content = "A" * L
    f = self.fs.open_file(path, os.O_WRONLY, blocksize=blocksize)
    byte_count = f.write(content)
    self.assertEqual(byte_count, L)
    f.close()
    f = self.fs.open_file(path)
    read_content = f.read()
    f.close()
    self.fs.delete(path)
    self.assertEqual(read_content, content)

  def copy(self):
    pass

  def copy_on_self(self):
    path = 'foobar.txt'
    path1 = 'foobar1.txt'
    txt  = 'hello there!'
    N = 10
    data = self._write_example_file(path, N, txt, self.fs)
    self.fs.copy(path, self.fs, path1)
    self.fs.delete(path)
    self.assertFalse(self.fs.exists(path))
    self.assertTrue(self.fs.exists(path1))
    f = self.fs.open_file(path1)
    data2 = f.read()
    self.assertEqual(len(data2), len(data), "wrong number of bytes read.")
    self.assertEqual(data2, data, "wrong bytes read.")
    f.close()
    self.fs.delete(path1)

  def move(self):
    pass

  def rename(self):
    old_path = 'foobar.txt'
    new_path = 'MOVED-' + old_path
    txt = 'hello there!'
    N = 100
    _ = self._write_example_file(old_path, N, txt)
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
    _ = [self._write_example_file(p, N, txt) for p in paths]
    infos = self.fs.list_directory(new_d)
    self.assertEqual(len(infos), len(paths))
    for i in infos:
      self.__check_path_info(i, kind="file", size=N*len(txt))
      self.assertTrue(i['name'].rsplit("/",1)[1] in basenames)
    for p in paths:
      self.fs.delete(p)
    self.fs.delete(os.path.join(cwd, parts[0]))
    self.assertRaises(IOError, self.fs.list_directory, new_d)

  def __check_readline(self, get_lines):
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
        f = self.fs.open_file(path, os.O_WRONLY, 0, 0, 0, chunk_size)
        f.write(text)
        f.close()
        f = self.fs.open_file(path, os.O_RDONLY, 0, 0, 0, chunk_size)
        lines = get_lines(f)
        self.assertEqual(lines, expected_lines)
        f.close()
    self.fs.delete(path)

  def readline(self):
    def get_lines(f):
      lines = []
      while 1:
        l = f.readline()
        if l == "":
          break
        lines.append(l)
      return lines
    self.__check_readline(get_lines)

  def iter_lines(self):
    def get_lines_explicit(f):
      lines = []
      while 1:
        try:
          lines.append(f.next())
        except StopIteration:
          break
      return lines
    def get_lines_implicit(f):
      return [l for l in f]
    for fun in get_lines_explicit, get_lines_implicit:
      self.__check_readline(fun)

  def seek(self):
    lines = ["1\n", "2\n", "3\n"]
    text = "".join(lines)
    path = "foobar.txt"
    for chunk_size in range(1, 2+len(text)):
      with self.fs.open_file(path, "w") as f:
        f.write(text)
      with self.fs.open_file(path, readline_chunk_size=chunk_size) as f:
        for i, l in enumerate(lines):
          f.seek(sum(map(len, lines[:i])))
          self.assertEqual(f.readline(), lines[i])
          f.seek(0)
          self.assertEqual(f.readline(), lines[0])
          f.seek(sum(map(len, lines[:i])))
          self.assertEqual(f.readline(), lines[i])
      with self.fs.open_file(path) as f:
        f.seek(1)
        f.seek(1, os.SEEK_CUR)
        self.assertEqual(f.tell(), 2)
        f.seek(-1, os.SEEK_END)
        self.assertEqual(f.tell(), len(text)-1)
    self.fs.delete(path)

  def block_boundary(self):
    path = "/tmp/foo"
    CHUNK_SIZE = 10
    N = 2
    bs = N * self.__get_bytes_per_checksum()
    total_data = 2 * bs
    f = self.fs.open_file(path, os.O_WRONLY, blocksize=bs)
    f.write("".join([chr(random.randint(32,126)) for _ in xrange(total_data)]))
    f.close()
    f = self.fs.open_file(path)
    try:
      p = total_data - CHUNK_SIZE
      for pos in 0, 1, bs-1, bs, bs+1, p-1, p, p+1, total_data-1:
        expected_len = CHUNK_SIZE if pos <= p else total_data - pos
        f.seek(pos)
        chunk = f.read(CHUNK_SIZE)
        self.assertEqual(len(chunk), expected_len)
    finally:
      f.close()
      self.fs.delete(path)

  def top_level_open(self):
    pass

  def __check_path_info(self, info, **expected_values):
    keys = ('kind', 'group', 'name', 'last_mod', 'replication', 'owner',
            'permissions', 'block_size', 'last_access', 'size')
    for k in keys:
      self.assertTrue(k in info)
    for k, exp_v in expected_values.iteritems():
      v = info[k]
      self.assertEqual(v, exp_v)

  def __get_bytes_per_checksum(self):

    def extract_text(nodes):
      return str("".join([n.data for n in nodes if n.nodeType == n.TEXT_NODE]))

    def extract_bpc(conf_path):
      dom = xml.dom.minidom.parse(conf_path)
      conf = dom.getElementsByTagName("configuration")[0]
      props = conf.getElementsByTagName("property")
      for p in props:
        n = p.getElementsByTagName("name")[0]
        if extract_text(n.childNodes) == "io.bytes.per.checksum":
          v = p.getElementsByTagName("value")[0]
          return int(extract_text(v.childNodes))
      raise IOError  # for consistency, also raised by minidom.path

    core_default = os.path.join(HADOOP_HOME, "src", "core", "core-default.xml")
    core_site, hadoop_site = [os.path.join(HADOOP_CONF_DIR, fn) for fn in
                              ("core-site.xml", "hadoop-site.xml")]
    try:
      return extract_bpc(core_site)
    except IOError:
      try:
        return extract_bpc(hadoop_site)
      except IOError:
        try:
          return extract_bpc(core_default)
        except IOError:
          return self.DEFAULT_BYTES_PER_CHECKSUM


def basic_tests():
  return [
    'delete',
    'chmod',
    'connect',
    'open_close',
    'file_attrs',
    'flush',
    'write_read',
    'write_read_chunk',
    'read_all',
    'rename',
    'change_dir',
    'create_dir',
    'copy_on_self',
    'available',
    'get_path_info',
    'list_directory',
    'readline',
    'iter_lines',
    'seek',
    'block_boundary',
    'top_level_open',
    ]
