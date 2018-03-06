# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import sys
import os
import unittest
import uuid
import shutil
import operator
import array
from ctypes import create_string_buffer

import pydoop.hdfs as hdfs
import pydoop
import pydoop.test_utils as utils
from pydoop.utils.py3compat import _is_py3


class TestCommon(unittest.TestCase):

    def __init__(self, target, hdfs_host='', hdfs_port=0):
        unittest.TestCase.__init__(self, target)
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port

    def setUp(self):
        self.fs = hdfs.hdfs(self.hdfs_host, self.hdfs_port)
        self.wd = utils.make_wd(self.fs)

    def tearDown(self):
        self.fs.delete(self.wd)
        self.fs.close()

    def _make_random_path(self, where=None):
        return "%s/%s_%s" % (where or self.wd, uuid.uuid4().hex, utils.UNI_CHR)

    # also an implicit test for the create_directory method
    def _make_random_dir(self, where=None):
        path = self._make_random_path(where=where)
        self.fs.create_directory(path)
        self.assertTrue(self.fs.exists(path))
        return path

    # also an implicit test for the write method
    def _make_random_file(self, where=None, content=None, **kwargs):
        kwargs["mode"] = "w"
        content = content or utils.make_random_data(printable=True)
        path = self._make_random_path(where=where)
        with self.fs.open_file(path, **kwargs) as fo:
            i = 0
            bytes_written = 0
            bufsize = 24 * 1024 * 1024
            while i < len(content):
                bytes_written += fo.write(content[i: i + bufsize])
                i += bufsize

        self.assertEqual(bytes_written, len(content))
        return path

    def failUnlessRaisesExternal(self, excClass, callableObj, *args, **kwargs):
        utils.silent_call(
            self.failUnlessRaises, excClass, callableObj, *args, **kwargs
        )

    assertRaisesExternal = failUnlessRaisesExternal

    def open_close(self):
        path = self._make_random_path()
        self.fs.open_file(path, "w").close()
        with self.fs.open_file(path, "r") as f:
            self.assertFalse(f.closed)
        self.assertTrue(f.closed)
        self.assertRaises(ValueError, f.read)
        path = self._make_random_path()
        self.assertRaisesExternal(IOError, self.fs.open_file, path, "r")
        self.assertRaises(ValueError, self.fs.open_file, "")

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
        self.assertRaises(ValueError, self.fs.delete, "")

    def copy(self):
        local_fs = hdfs.hdfs('', 0)
        local_wd = utils.make_wd(local_fs)
        from_path = os.path.join(local_wd, uuid.uuid4().hex)
        content = uuid.uuid4().bytes
        with open(from_path, "wb") as f:
            f.write(content)
        to_path = self._make_random_file()
        local_fs.copy(from_path, self.fs, to_path)
        self.assertRaises(ValueError, local_fs.copy, "", self.fs, "")
        local_fs.close()
        with self.fs.open_file(to_path) as f:
            self.assertEqual(f.read(), content)
        shutil.rmtree(local_wd)

    def move(self):
        content = utils.make_random_data(printable=True)
        from_path = self._make_random_file(content=content)
        to_path = self._make_random_path()
        self.fs.move(from_path, self.fs, to_path)
        self.assertFalse(self.fs.exists(from_path))
        with self.fs.open_file(to_path) as f:
            self.assertEqual(f.read(), content)
        self.assertRaises(ValueError, self.fs.move, "", self.fs, "")

    def chmod(self):
        new_perm = 0o777
        path = self._make_random_dir()
        old_perm = self.fs.get_path_info(path)["permissions"]
        assert old_perm != new_perm
        self.fs.chmod(path, new_perm)
        self.assertEqual(self.fs.get_path_info(path)["permissions"], new_perm)
        self.fs.chmod(path, old_perm)
        self.assertEqual(self.fs.get_path_info(path)["permissions"], old_perm)
        self.assertRaises(ValueError, self.fs.chmod, "", new_perm)

    def __set_and_check_perm(self, path, new_mode, expected_mode):
        self.fs.chmod(path, new_mode)
        perm = self.fs.get_path_info(path)["permissions"]
        self.assertEqual(expected_mode, perm)

    def chmod_w_string(self):
        path = self._make_random_dir()
        self.fs.chmod(path, 0o500)
        # each user
        self.__set_and_check_perm(path, "u+w", 0o700)
        self.__set_and_check_perm(path, "g+w", 0o720)
        self.__set_and_check_perm(path, "o+w", 0o722)
        # each permission mode
        self.__set_and_check_perm(path, "o+r", 0o726)
        self.__set_and_check_perm(path, "o+x", 0o727)
        # subtract operation, and multiple permission modes
        self.__set_and_check_perm(path, "o-rwx", 0o720)
        # multiple users
        self.__set_and_check_perm(path, "ugo-rwx", 0o000)
        # 'a' user
        self.__set_and_check_perm(path, "a+r", 0o444)
        # blank user -- should respect the user's umask
        umask = os.umask(0o007)
        self.fs.chmod(path, "+w")
        perm = self.fs.get_path_info(path)["permissions"]
        os.umask(umask)
        self.assertEqual(0o664, perm)
        # assignment op
        self.__set_and_check_perm(path, "a=rwx", 0o777)

    def file_attrs(self):
        path = self._make_random_path()
        content = utils.make_random_data()
        for mode in "wb", "wt":
            with self.fs.open_file(path, mode) as f:
                self.assertTrue(f.name.endswith(path))
                self.assertTrue(f.fs is self.fs)
                self.assertEqual(f.size, 0)
                self.assertEqual(f.mode, mode)
                self.assertTrue(f.writable())
                f.write(content if mode == "wb" else content.decode("utf-8"))
            self.assertEqual(f.size, len(content))
        for mode in "rb", "rt":
            with self.fs.open_file(path, mode) as f:
                self.assertTrue(f.name.endswith(path))
                self.assertTrue(f.fs is self.fs)
                self.assertEqual(f.size, len(content))
                self.assertEqual(f.mode, mode)
                self.assertFalse(f.writable())

    def flush(self):
        path = self._make_random_path()
        with self.fs.open_file(path, "w") as f:
            f.write(utils.make_random_data())
            f.flush()

    def available(self):
        content = utils.make_random_data()
        path = self._make_random_file(content=content)
        with self.fs.open_file(path) as f:
            self.assertEqual(len(content), f.available())

    def get_path_info(self):
        content = utils.make_random_data()
        path = self._make_random_file(content=content)
        info = self.fs.get_path_info(path)
        self.__check_path_info(info, kind="file", size=len(content))
        self.assertTrue(info['name'].endswith(path))
        path = self._make_random_dir()
        info = self.fs.get_path_info(path)
        self.__check_path_info(info, kind="directory")
        self.assertTrue(info['name'].endswith(path))
        self.assertRaises(
            IOError, self.fs.get_path_info, self._make_random_path()
        )
        self.assertRaises(ValueError, self.fs.get_path_info, "")

    def read(self):
        content = utils.make_random_data()
        path = self._make_random_file(content=content)
        with self.fs.open_file(path) as f:
            self.assertEqual(f.read(), content)
        with self.fs.open_file(path) as f:
            self.assertEqual(f.read(-1), content)
        with self.fs.open_file(path) as f:
            self.assertEqual(f.read(3), content[:3])
            self.assertEqual(f.read(3), content[3:6])
            if not _is_py3 and not self.fs.host:
                self.assertRaises(ValueError, f.write, content)
            else:
                self.assertRaises(IOError, f.write, content)

    def __read_chunk(self, chunk_factory):
        content = utils.make_random_data()
        path = self._make_random_file(content=content)
        size = len(content)
        for chunk_size in size - 1, size, size + 1:
            with self.fs.open_file(path) as f:
                chunk = chunk_factory(chunk_size)
                bytes_read = f.read_chunk(chunk)
                self.assertEqual(bytes_read, min(size, chunk_size))
                self.assertEqual(bytes(bytearray(chunk))[:bytes_read],
                                 content[:bytes_read])

    def read_chunk(self):
        def array_by_len(length):
            return array.array("b", b"\x00" * length)
        for factory in bytearray, create_string_buffer, array_by_len:
            self.__read_chunk(factory)

    def write(self):
        content = utils.make_random_data()
        path = self._make_random_path()
        with self.fs.open_file(path, "w") as fo:
            bytes_written = fo.write(content)
            self.assertEqual(bytes_written, len(content))
        with self.fs.open_file(path) as fo:
            self.assertEqual(content, fo.read())
        with self.fs.open_file(path, "w") as fo:
            bytes_written = fo.write(bytearray(content))
            self.assertEqual(bytes_written, len(content))
        with self.fs.open_file(path) as fo:
            self.assertEqual(content, fo.read())
        chunk = create_string_buffer(content, len(content))
        with self.fs.open_file(path, "w") as fo:
            bytes_written = fo.write(chunk)
            self.assertEqual(bytes_written, len(content))

    def append(self):
        replication = 1  # see https://issues.apache.org/jira/browse/HDFS-3091
        content, update = utils.make_random_data(), utils.make_random_data()
        path = self._make_random_path()
        with self.fs.open_file(path, "w", replication=replication) as fo:
            fo.write(content)
        try:
            with utils.silent_call(self.fs.open_file, path, "a") as fo:
                fo.write(update)
        except IOError:
            sys.stderr.write("NOT SUPPORTED ... ")
            return
        else:
            with self.fs.open_file(path) as fi:
                self.assertEqual(fi.read(), content + update)

    def tell(self):
        offset = 3
        path = self._make_random_file()
        with self.fs.open_file(path) as f:
            f.read(offset)
            self.assertEqual(f.tell(), offset)

    def pread(self):
        content = utils.make_random_data()
        offset, length = 2, 3
        path = self._make_random_file(content=content)
        with self.fs.open_file(path) as f:
            self.assertEqual(
                f.pread(offset, length), content[offset: offset + length]
            )
            self.assertEqual(f.tell(), 0)
            self.assertEqual(content[1:], f.pread(1, -1))
            self.assertRaises(IOError, f.pread, -1, 10)
            # read starting past end of file
            self.assertRaises(IOError, f.pread, len(content) + 1, 10)
            # read past end of file
            buf = f.pread(len(content) - 2, 10)
            self.assertEqual(2, len(buf))

    def pread_chunk(self):
        content = utils.make_random_data()
        offset, length = 2, 3
        chunk = create_string_buffer(length)
        path = self._make_random_file(content=content)
        with self.fs.open_file(path) as f:
            bytes_read = f.pread_chunk(offset, chunk)
            self.assertEqual(bytes_read, length)
            self.assertEqual(chunk.value, content[offset: offset + length])
            self.assertEqual(f.tell(), 0)

    def copy_on_self(self):
        content = utils.make_random_data()
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
        self.assertRaises(ValueError, self.fs.rename, old_path, "")
        self.assertRaises(ValueError, self.fs.rename, "", new_path)

    def change_dir(self):
        cwd = self.fs.working_directory()
        new_d = self._make_random_path()  # does not need to exist
        self.fs.set_working_directory(new_d)
        self.assertEqual(self.fs.working_directory(), new_d)
        self.fs.set_working_directory(cwd)
        self.assertEqual(self.fs.working_directory(), cwd)
        self.assertRaises(ValueError, self.fs.set_working_directory, "")

    def list_directory(self):
        new_d = self._make_random_dir()
        self.assertEqual(self.fs.list_directory(new_d), [])
        paths = [self._make_random_file(where=new_d) for _ in range(3)]
        paths.sort(key=os.path.basename)
        infos = self.fs.list_directory(new_d)
        infos.sort(key=lambda p: os.path.basename(p["name"]))
        self.assertEqual(len(infos), len(paths))
        for i, p in zip(infos, paths):
            self.__check_path_info(i, kind="file")
            self.assertTrue(i['name'].endswith(p))
        self.assertRaises(
            IOError, self.fs.list_directory, self._make_random_path()
        )
        self.assertRaises(ValueError, self.fs.list_directory, "")

    def __check_readline(self, get_lines):
        samples = [
            b"foo\nbar\n\ntar",
            b"\nfoo\nbar\n\ntar",
            b"foo\nbar\n\ntar\n",
            b"\n\n\n", b"\n", b"",
            b"foobartar",
        ]
        path = self._make_random_path()
        for text in samples:
            expected_lines = text.splitlines(True)
            with self.fs.open_file(path, "w") as f:
                f.write(text)
            with self.fs.open_file(path) as f:
                lines = get_lines(f)
            self.assertEqual(lines, expected_lines)

    def readline(self):
        def get_lines(f):
            lines = []
            while 1:
                line = f.readline()
                if not line:
                    break
                lines.append(line)
            return lines
        self.__check_readline(get_lines)

    def readline_big(self):
        for i in range(10, 23):
            x = b"*" * (2**i) + b"\n"
            path = self._make_random_file(content=x)
            with self.fs.open_file(path) as f:
                line = f.readline()
            self.assertEqual(
                line, x, "len(a) = %d, len(x) = %d" % (len(line), len(x))
            )

    def readline_and_read(self):
        content = b"first line\nsecond line\n"
        path = self._make_random_file(content=content)
        chunks = []
        with self.fs.open_file(path) as f:
            chunks.append(f.read(1))
            chunks.append(f.readline())
            chunks.append(f.read(4))
        self.assertEqual(chunks, [b'f', b'irst line\n', b'seco'])

    def iter_lines(self):

        def get_lines_explicit(f):
            lines = []
            while 1:
                try:
                    lines.append(next(f))
                except StopIteration:
                    break
            return lines

        def get_lines_implicit(f):
            return [l for l in f]

        for fun in get_lines_explicit, get_lines_implicit:
            self.__check_readline(fun)

    def seek(self):
        lines = [b"1\n", b"2\n", b"3\n"]
        data = b"".join(lines)
        path = self._make_random_path()
        with self.fs.open_file(path, "w") as f:
            f.write(data)
        with self.fs.open_file(path) as f:
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
            self.assertEqual(f.tell(), len(data) - 1)
            # seek past end of file
            self.assertRaises(IOError, f.seek, len(data) + 10)

    def block_boundary(self):
        hd_info = pydoop.hadoop_version_info()
        path = self._make_random_path()
        CHUNK_SIZE = 10
        N = 2
        kwargs = {}
        if hd_info.has_deprecated_bs():
            bs = hdfs.fs.hdfs().default_block_size()
        else:
            # (dfs.namenode.fs-limits.min-block-size): 4096 < 1048576
            bs = max(1048576, N * utils.get_bytes_per_checksum())
            kwargs['blocksize'] = bs
        total_data_size = 2 * bs
        with self.fs.open_file(path, "w", **kwargs) as f:
            i = 0
            bufsize = 12 * 1024 * 1024
            while i < total_data_size:
                data = b'X' * min(bufsize, total_data_size - i)
                f.write(data)
                i += bufsize

        with self.fs.open_file(path) as f:
            p = total_data_size - CHUNK_SIZE
            for pos in (0, 1, bs - 1, bs, bs + 1, p - 1, p, p + 1,
                        total_data_size - 1):
                expected_len = (
                    CHUNK_SIZE if pos <= p else total_data_size - pos
                )
                f.seek(pos)
                chunk = f.read(CHUNK_SIZE)
                self.assertEqual(len(chunk), expected_len)

    def walk(self):
        new_d, new_f = self._make_random_dir(), self._make_random_file()
        for top in new_d, new_f:
            self.assertEqual(
                list(self.fs.walk(top)), [self.fs.get_path_info(top)]
            )
        top = new_d
        cache = [top]
        for _ in range(2):
            cache.append(self._make_random_file(where=top))
        parent = self._make_random_dir(where=top)
        cache.append(parent)
        for _ in range(2):
            cache.append(self._make_random_file(where=parent))
        child = self._make_random_dir(where=parent)
        cache.append(child)
        for _ in range(2):
            cache.append(self._make_random_file(where=child))
        infos = list(self.fs.walk(top))
        expected_infos = [self.fs.get_path_info(p) for p in cache]
        self.assertEqual(len(infos), len(expected_infos))
        for l in infos, expected_infos:
            l.sort(key=operator.itemgetter("name"))
        self.assertEqual(infos, expected_infos)
        nonexistent_walk = self.fs.walk(self._make_random_path())
        if _is_py3:
            self.assertRaises(OSError, lambda: next(nonexistent_walk))
        else:
            self.assertRaises(IOError, lambda: next(nonexistent_walk))
        for top in '', None:
            self.assertRaises(ValueError, lambda: next(self.fs.walk(top)))

    def exists(self):
        self.assertFalse(self.fs.exists('some_file'))
        self.assertFalse(self.fs.exists('some_file/other_file'))
        dname = self._make_random_dir()
        self.assertTrue(self.fs.exists(dname))
        fname = self._make_random_file()
        self.assertTrue(self.fs.exists(fname))
        self.assertRaises(ValueError, self.fs.exists, "")

    def text_io(self):
        t_path, b_path = self._make_random_path(), self._make_random_path()
        text = u'a string' + utils.UNI_CHR
        data = text.encode("utf-8")
        with self.fs.open_file(t_path, "wt") as fo:
            chars_written = fo.write(text)
        with self.fs.open_file(b_path, "w") as fo:
            bytes_written = fo.write(data)
        self.assertEqual(chars_written, len(text))
        self.assertEqual(bytes_written, len(data))
        with self.fs.open_file(t_path, "rt") as f:
            self.assertEqual(f.read(), text)
            f.seek(2)
            self.assertEqual(f.read(), text[2:])
            self.assertEqual(f.pread(3, 4), text[3:7])
            with self.assertRaises(AttributeError):
                f.read_chunk("")
                f.pread_chunk(1, "")
        with self.fs.open_file(b_path, "r") as f:
            self.assertEqual(f.read(), data)

    def __check_path_info(self, info, **expected_values):
        keys = ('kind', 'group', 'name', 'last_mod', 'replication', 'owner',
                'permissions', 'block_size', 'last_access', 'size')
        for k in keys:
            self.assertTrue(k in info)
        for k, exp_v in list(expected_values.items()):
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
        'write',
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
        'readline_and_read',
        'iter_lines',
        'seek',
        'block_boundary',
        'walk',
        'exists',
        'text_io',
    ]
