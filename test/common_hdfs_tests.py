# BEGIN_COPYRIGHT
#
# Copyright 2009-2013 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import sys, os, unittest, uuid, shutil, operator
from itertools import izip
from ctypes import create_string_buffer

import pydoop.hdfs as hdfs
import pydoop
from utils import make_wd, make_random_data, get_bytes_per_checksum, silent_call

class TestCommon(unittest.TestCase):

  def __init__(self, target, hdfs_host='', hdfs_port=0):
    unittest.TestCase.__init__(self, target)
    self.hdfs_host = hdfs_host
    self.hdfs_port = hdfs_port

  def setUp(self):
    self.fs = hdfs.hdfs(self.hdfs_host, self.hdfs_port)
    self.wd = make_wd(self.fs)

  def tearDown(self):
    self.fs.delete(self.wd)
    self.fs.close()

  def _make_random_path(self, where=None):
    return "%s/%s" % (where or self.wd, uuid.uuid4().hex)

  # also an implicit test for the create_directory method
  def _make_random_dir(self, where=None):
    path = self._make_random_path(where=where)
    res = self.fs.create_directory(path)
    self.assertTrue(self.fs.exists(path))
    return path

  # also an implicit test for the write method
  def _make_random_file(self, where=None, content=None, **kwargs):
    kwargs["flags"] = "w"
    content = content or make_random_data()
    path = self._make_random_path(where=where)
    with self.fs.open_file(path, **kwargs) as fo:
      i = 0
      bytes_written = 0
      bufsize = hdfs.common.BUFSIZE
      while i < len(content):
        bytes_written += fo.write(content[i:i+bufsize])
        i += bufsize

    self.assertEqual(bytes_written, len(content))
    return path

  def failUnlessRaisesExternal(self, excClass, callableObj, *args, **kwargs):
    silent_call(self.failUnlessRaises, excClass, callableObj, *args, **kwargs)

  assertRaisesExternal = failUnlessRaisesExternal

  def open_close(self):
    for flags in "w", os.O_WRONLY:
      path = self._make_random_path()
      self.fs.open_file(path, flags).close()
      for flags in "r", os.O_RDONLY:
        f = self.fs.open_file(path, flags)
        self.assertFalse(f.closed)
        f.close()
        self.assertTrue(f.closed)
        self.assertRaises(ValueError, f.read)
    path = self._make_random_path()
    self.assertRaisesExternal(IOError, self.fs.open_file, path, "r")

  def delete(self):
    parent = self._make_random_dir()
    path = self._make_random_dir(where=parent)
    for i in False, True:
      fn = self._make_random_file(where=path)
      self.fs.delete(fn, recursive=i)
      self.assertFalse(self.fs.exists(fn))
    self._make_random_file(where=path)
    self.fs.delete(path, recursive=True)
    self.assertFalse(self.fs.exists(path))
    self.fs.delete(parent, recursive=False)
    self.assertFalse(self.fs.exists(parent))

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

  def chmod(self):
    new_perm = 0777
    path = self._make_random_dir()
    old_perm = self.fs.get_path_info(path)["permissions"]
    assert old_perm != new_perm
    self.fs.chmod(path, new_perm)
    self.assertEqual(self.fs.get_path_info(path)["permissions"], new_perm)
    self.fs.chmod(path, old_perm)
    self.assertEqual(self.fs.get_path_info(path)["permissions"], old_perm)

  def __set_and_check_perm(self, path, new_mode, expected_mode):
    self.fs.chmod(path, new_mode)
    perm = self.fs.get_path_info(path)["permissions"]
    self.assertEqual(expected_mode, perm)

  def chmod_w_string(self):
    path = self._make_random_dir()
    self.fs.chmod(path, 0500)
    # each user
    self.__set_and_check_perm(path, "u+w", 0700)
    self.__set_and_check_perm(path, "g+w", 0720)
    self.__set_and_check_perm(path, "o+w", 0722)
    # each permission mode
    self.__set_and_check_perm(path, "o+r", 0726)
    self.__set_and_check_perm(path, "o+x", 0727)
    # subtract operation, and multiple permission modes
    self.__set_and_check_perm(path, "o-rwx", 0720)
    # multiple users
    self.__set_and_check_perm(path, "ugo-rwx", 0000)
    # 'a' user
    self.__set_and_check_perm(path, "a+r", 0444)
    # blank user -- should respect the user's umask
    umask = os.umask(0007)
    self.fs.chmod(path, "+w")
    perm = self.fs.get_path_info(path)["permissions"]
    os.umask(umask)
    self.assertEqual(0664, perm)
    # assignment op
    self.__set_and_check_perm(path, "a=rwx", 0777)

  def file_attrs(self):
    path = self._make_random_path()
    with self.fs.open_file(path, os.O_WRONLY) as f:
      self.assertTrue(f.name.endswith(path))
      self.assertEqual(f.size, 0)
      self.assertEqual(f.mode, "w")
      content = make_random_data()
      f.write(content)
    self.assertEqual(f.size, len(content))
    with self.fs.open_file(path) as f:
      self.assertTrue(f.name.endswith(path))
      self.assertEqual(f.size, len(content))
      self.assertEqual(f.mode, "r")

  def flush(self):
    path = self._make_random_path()
    with self.fs.open_file(path, "w") as f:
      f.write(make_random_data())
      f.flush()

  def available(self):
    content = make_random_data()
    path = self._make_random_file(content=content)
    with self.fs.open_file(path) as f:
      self.assertEqual(len(content), f.available())

  def get_path_info(self):
    content = make_random_data()
    path = self._make_random_file(content=content)
    info = self.fs.get_path_info(path)
    self.__check_path_info(info, kind="file", size=len(content))
    self.assertTrue(info['name'].endswith(path))
    path = self._make_random_dir()
    info = self.fs.get_path_info(path)
    self.__check_path_info(info, kind="directory")
    self.assertTrue(info['name'].endswith(path))
    self.assertRaises(IOError, self.fs.get_path_info, self._make_random_path())

  def read(self):
    content = make_random_data()
    path = self._make_random_file(content=content)
    with self.fs.open_file(path) as f:
      self.assertEqual(f.read(), content)
    with self.fs.open_file(path) as f:
      self.assertEqual(f.read(3), content[:3])
      self.assertRaisesExternal(IOError, f.write, content)

  def read_chunk(self):
    content = make_random_data()
    path = self._make_random_file(content=content)
    size = len(content)
    for chunk_size in size-1, size, size+1:
      with self.fs.open_file(path) as f:
        chunk = create_string_buffer(chunk_size)
        bytes_read = f.read_chunk(chunk)
        self.assertEqual(bytes_read, min(size, chunk_size))
        self.assertEqual(chunk.value, content[:bytes_read])

  def write_chunk(self):
    content = make_random_data()
    chunk = create_string_buffer(len(content))
    chunk[:] = content
    path = self._make_random_path()
    with self.fs.open_file(path, "w") as fo:
      bytes_written = fo.write_chunk(chunk)
      self.assertEqual(bytes_written, len(content))
    return path

  def append(self):
    replication = 1  # see https://issues.apache.org/jira/browse/HDFS-3091
    content, update = make_random_data(), make_random_data()
    path = self._make_random_path()
    with self.fs.open_file(path, "w", replication=replication) as fo:
      fo.write(content)
    try:
      with silent_call(self.fs.open_file, path, "a") as fo:
        fo.write(update)
    except IOError:
      sys.stderr.write("NOT SUPPORTED ... ")
      return
    else:
      with self.fs.open_file(path) as fi:
        self.assertEqual(fi.read(), content+update)

  def tell(self):
    offset = 3
    path = self._make_random_file()
    with self.fs.open_file(path) as f:
      f.read(offset)
      self.assertEqual(f.tell(), offset)

  def pread(self):
    content = make_random_data()
    offset, length = 2, 3
    path = self._make_random_file(content=content)
    with self.fs.open_file(path) as f:
      self.assertEqual(f.pread(offset, length), content[offset:offset+length])
      self.assertEqual(f.tell(), 0)

  def pread_chunk(self):
    content = make_random_data()
    offset, length = 2, 3
    chunk = create_string_buffer(length)
    path = self._make_random_file(content=content)
    with self.fs.open_file(path) as f:
      bytes_read = f.pread_chunk(offset, chunk)
      self.assertEqual(bytes_read, length)
      self.assertEqual(chunk.value, content[offset:offset+length])
      self.assertEqual(f.tell(), 0)

  def copy_on_self(self):
    content = make_random_data()
    path = self._make_random_file(content=content)
    path1 = self._make_random_path()
    self.fs.copy(path, self.fs, path1)
    with self.fs.open_file(path1) as f:
      self.assertEqual(f.read(), content)

  def rename(self):
    old_path = self._make_random_file()
    new_path = self._make_random_path()
    self.fs.rename(old_path, new_path)
    self.assertTrue(self.fs.exists(new_path))
    self.assertFalse(self.fs.exists(old_path))

  def change_dir(self):
    cwd = self.fs.working_directory()
    new_d = self._make_random_path()  # does not need to exist
    self.fs.set_working_directory(new_d)
    self.assertEqual(self.fs.working_directory(), new_d)
    self.fs.set_working_directory(cwd)
    self.assertEqual(self.fs.working_directory(), cwd)

  def list_directory(self):
    new_d = self._make_random_dir()
    
    self.assertEqual(self.fs.list_directory(new_d), [])
    paths = [self._make_random_file(where=new_d) for _ in xrange(3)]
    paths.sort(key=os.path.basename)
    infos = self.fs.list_directory(new_d)
    infos.sort(key=lambda p: os.path.basename(p["name"]))
    self.assertEqual(len(infos), len(paths))
    for i, p in izip(infos, paths):
      self.__check_path_info(i, kind="file")
      self.assertTrue(i['name'].endswith(p))
    self.assertRaises(IOError, self.fs.list_directory, self._make_random_path())

  def __check_readline(self, get_lines):
    samples = [
      "foo\nbar\n\ntar",
      "\nfoo\nbar\n\ntar",
      "foo\nbar\n\ntar\n",
      "\n\n\n", "\n", "",
      "foobartar",
      ]
    path = self._make_random_path()
    for text in samples:
      expected_lines = text.splitlines(True)
      for chunk_size in 2, max(1, len(text)), 2+len(text):
        with self.fs.open_file(path, "w") as f:
          f.write(text)
        with self.fs.open_file(path, readline_chunk_size=chunk_size) as f:
          lines = get_lines(f)
        self.assertEqual(lines, expected_lines)

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

  def readline_big(self):
    for i in xrange(10, 23):
      x = '*' * (2**i) + "\n"
      path = self._make_random_file(content=x)
      with self.fs.open_file(path) as f:
        l = f.readline()
      self.assertEqual(l, x, "len(a) = %d, len(x) = %d" % (len(l), len(x)))

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
    path = self._make_random_path()
    for chunk_size in range(1, 2+len(text)):
      with self.fs.open_file(path, "w") as f:
        f.write(text)
      with self.fs.open_file(path, readline_chunk_size=chunk_size) as f:
        for i, l in enumerate(lines):
          f.seek(sum(map(len, lines[:i])))
          self.assertEqual(f.readline(), l)
          f.seek(0)
          self.assertEqual(f.readline(), lines[0])
          f.seek(sum(map(len, lines[:i])))
          self.assertEqual(f.readline(), l)
      with self.fs.open_file(path) as f:
        f.seek(1)
        f.seek(1, os.SEEK_CUR)
        self.assertEqual(f.tell(), 2)
        f.seek(-1, os.SEEK_END)
        self.assertEqual(f.tell(), len(text)-1)

  def block_boundary(self):
    path = self._make_random_path()
    CHUNK_SIZE = 10
    N = 2
    kwargs = {}
    if pydoop.hadoop_version_info().has_deprecated_bs():
        bs = hdfs.fs.hdfs().default_block_size()
    else:
        bs = N * get_bytes_per_checksum()
        kwargs['blocksize'] = bs
    total_data_size = 2 * bs
    with self.fs.open_file(path, "w", **kwargs) as f:
      data = make_random_data(total_data_size)
      i = 0
      bufsize = hdfs.common.BUFSIZE
      while i < len(data):
        f.write(data[i:i+bufsize])
        i += bufsize

    with self.fs.open_file(path) as f:
      p = total_data_size - CHUNK_SIZE
      for pos in 0, 1, bs-1, bs, bs+1, p-1, p, p+1, total_data_size-1:
        expected_len = CHUNK_SIZE if pos <= p else total_data_size - pos
        f.seek(pos)
        chunk = f.read(CHUNK_SIZE)
        self.assertEqual(len(chunk), expected_len)

  def walk(self):
    new_d, new_f = self._make_random_dir(), self._make_random_file()
    for top in new_d, new_f:
      self.assertEqual(list(self.fs.walk(top)), [self.fs.get_path_info(top)])
    top = new_d
    cache = [top]
    for _ in xrange(2):
      cache.append(self._make_random_file(where=top))
    parent = self._make_random_dir(where=top)
    cache.append(parent)
    for _ in xrange(2):
      cache.append(self._make_random_file(where=parent))
    child = self._make_random_dir(where=parent)
    cache.append(child)
    for _ in xrange(2):
      cache.append(self._make_random_file(where=child))
    infos = list(self.fs.walk(top))
    expected_infos = [self.fs.get_path_info(p) for p in cache]
    self.assertEqual(len(infos), len(expected_infos))
    for l in infos, expected_infos:
      l.sort(key=operator.itemgetter("name"))
    self.assertEqual(infos, expected_infos)
    nonexistent_walk = self.fs.walk(self._make_random_path())
    self.assertRaises(IOError, nonexistent_walk.next)

  def __check_path_info(self, info, **expected_values):
    keys = ('kind', 'group', 'name', 'last_mod', 'replication', 'owner',
            'permissions', 'block_size', 'last_access', 'size')
    for k in keys:
      self.assertTrue(k in info)
    for k, exp_v in expected_values.iteritems():
      v = info[k]
      self.assertEqual(v, exp_v)


def common_tests():
  return [
    'open_close',
    'delete',
    'copy',
    'move',
    'chmod',
    'chmod_w_string',
    'file_attrs',
    'flush',
    'read',
    'read_chunk',
    'write_chunk',
    'append',
    'tell',
    'pread',
    'pread_chunk',
    'rename',
    'change_dir',
    'copy_on_self',
    'available',
    'get_path_info',
    'list_directory',
    'readline',
    'readline_big',
    'iter_lines',
    'seek',
    'block_boundary',
    'walk',
    ]
