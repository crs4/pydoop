# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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

import unittest
import getpass
import socket
from itertools import product

import pydoop.hdfs as hdfs
import pydoop
from common_hdfs_tests import TestCommon, common_tests
import pydoop.test_utils as u
from pydoop.utils.py3compat import clong

CURRENT_USER = getpass.getuser()


class TestConnection(unittest.TestCase):

    def setUp(self):
        self.hp_cases = [("default", 0)]
        self.u_cases = [None, CURRENT_USER]
        if not hdfs.default_is_local():
            self.hp_cases.append((u.HDFS_HOST, u.HDFS_PORT))
            self.u_cases.append("nobody")
            try:
                hdfs_ip = socket.gethostbyname(u.HDFS_HOST)
            except socket.gaierror:
                pass
            else:
                self.hp_cases.append((hdfs_ip, u.HDFS_PORT))

    def connect(self):
        for host, port in self.hp_cases:
            for user in self.u_cases:
                expected_user = user or CURRENT_USER
                with hdfs.hdfs(host, port, user=user) as fs:
                    self.assertEqual(fs.user, expected_user)

    def cache(self):
        for (h1, p1), (h2, p2) in product(self.hp_cases, repeat=2):
            hdfs.hdfs._CACHE.clear()
            hdfs.hdfs._ALIASES = {"host": {}, "port": {}, "user": {}}  # FIXME
            with hdfs.hdfs(h1, p1) as fs1:
                with hdfs.hdfs(h2, p2) as fs2:
                    print(' * %r vs %r' % ((h1, p1), (h2, p2)))
                    self.assertTrue(fs2.fs is fs1.fs)
                for fs in fs1, fs2:
                    self.assertFalse(fs.closed)
            for fs in fs1, fs2:
                self.assertTrue(fs.closed)


class TestHDFS(TestCommon):

    def __init__(self, target):
        TestCommon.__init__(self, target, 'default', 0)

    def capacity(self):
        c = self.fs.capacity()
        self.assertTrue(isinstance(c, (int, clong)))

    def default_block_size(self):
        dbs = self.fs.default_block_size()
        self.assertTrue(isinstance(dbs, (int, clong)))

    def used(self):
        u_ = self.fs.used()
        self.assertTrue(isinstance(u_, (int, clong)))

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
        self.assertEqual(
            self.fs.get_path_info(path)["last_mod"], int(new_mtime)
        )
        self.assertEqual(
            self.fs.get_path_info(path)["last_access"], int(new_atime)
        )
        self.fs.utime(path, old_mtime, old_atime)
        self.assertEqual(
            self.fs.get_path_info(path)["last_mod"], int(old_mtime)
        )
        self.assertEqual(
            self.fs.get_path_info(path)["last_access"], int(old_atime)
        )

    def block_size(self):
        if not pydoop.hadoop_version_info().has_deprecated_bs():
            for bs_MB in range(100, 500, 50):
                bs = bs_MB * 2**20
                path = self._make_random_file(blocksize=bs)
                self.assertEqual(self.fs.get_path_info(path)["block_size"], bs)

    def replication(self):
        for r in range(1, 6):
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

        def _write_prefix(f, size, bs):
            # Avoid memory problem with JVM
            chunk_size = min(bs, 12 * 1048576)
            written = 0
            while written < size:
                data = b'X' * min(chunk_size, size - written)
                written += f.write(data)

        hd_info = pydoop.hadoop_version_info()
        kwargs = {}
        if hd_info.has_deprecated_bs():
            bs = hdfs.fs.hdfs().default_block_size()
        else:
            # (dfs.namenode.fs-limits.min-block-size): 4096 < 1048576
            bs = 1048576
            kwargs['blocksize'] = bs

        line = b"012345678\n"
        offset = bs - (10 * len(line) + 5)
        path = self._make_random_path()
        with self.fs.open_file(path, mode="w", **kwargs) as f:
            bytes_written = lines_written = 0
            _write_prefix(f, offset, bs)
            bytes_written = offset
            while bytes_written < bs + 1:
                f.write(line)
                lines_written += 1
                bytes_written += len(line)
        with self.fs.open_file(path) as f:
            f.seek(offset)
            lines = []
            while 1:
                L = f.readline()
                if not L:
                    break
                lines.append(L)
        self.assertEqual(len(lines), lines_written)
        for i, L in enumerate(lines):
            self.assertEqual(L, line, "line %d: %r != %r" % (i, L, line))

    def get_hosts(self):
        hd_info = pydoop.hadoop_version_info()
        kwargs = {}
        if hd_info.has_deprecated_bs() and not hd_info.is_cdh_v5():
            blocksize = hdfs.fs.hdfs().default_block_size()
        else:
            # (dfs.namenode.fs-limits.min-block-size): 4096 < 1048576
            blocksize = 1048576
            kwargs['blocksize'] = blocksize
        N = 4
        content = b"x" * blocksize * N
        path = self._make_random_file(content=content, **kwargs)
        start = 0
        for i in range(N):
            length = blocksize * i + 1
            hosts_per_block = self.fs.get_hosts(path, start, length)
            self.assertEqual(len(hosts_per_block), i + 1)


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestConnection('connect'))
    suite_.addTest(TestConnection('cache'))
    tests = common_tests()
    if not hdfs.default_is_local():
        tests.extend([
            'capacity',
            'default_block_size',
            'used',
            'chown',
            'utime',
            'block_size',
            'replication',
            'set_replication',
            'readline_block_boundary',
            'get_hosts',
        ])
    for t in tests:
        suite_.addTest(TestHDFS(t))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
