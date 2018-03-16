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

from __future__ import division

import unittest
import tempfile
import os
import stat
from pydoop.utils.py3compat import czip
from threading import Thread

import pydoop.hdfs as hdfs
from pydoop.hdfs.common import BUFSIZE
from pydoop.test_utils import UNI_CHR, make_random_data, FSTree


class TestHDFS(unittest.TestCase):

    def setUp(self):
        wd = tempfile.mkdtemp(suffix='_%s' % UNI_CHR)
        wd_bn = os.path.basename(wd)
        self.local_wd = "file:%s" % wd
        fs = hdfs.hdfs("default", 0)
        fs.create_directory(wd_bn)
        self.hdfs_wd = fs.get_path_info(wd_bn)["name"]
        fs.close()
        basenames = ["test_path_%d" % i for i in range(2)]
        self.local_paths = ["%s/%s" % (self.local_wd, bn) for bn in basenames]
        self.hdfs_paths = ["%s/%s" % (self.hdfs_wd, bn) for bn in basenames]
        self.data = make_random_data(
            4 * BUFSIZE + BUFSIZE // 2, printable=False
        )
        for path in self.local_paths:
            self.assertTrue(path.startswith("file:"))
        for path in self.hdfs_paths:
            if not hdfs.default_is_local():
                self.assertTrue(path.startswith("hdfs:"))

    def tearDown(self):
        fs = hdfs.hdfs("", 0)
        fs.delete(self.local_wd)
        fs.close()
        fs = hdfs.hdfs("default", 0)
        fs.delete(self.hdfs_wd)
        fs.close()

    def open(self):
        for test_path in self.hdfs_paths[0], self.local_paths[0]:
            with hdfs.open(test_path, "w") as f:
                f.write(self.data)
            f.fs.close()
            with hdfs.open(test_path) as f:
                self.assertEqual(f.read(), self.data)
            f.fs.close()

    def dump(self):
        for test_path in self.hdfs_paths[0], self.local_paths[0]:
            hdfs.dump(self.data, test_path, mode="wb")
            with hdfs.open(test_path) as fi:
                rdata = fi.read()
            fi.fs.close()
            self.assertEqual(rdata, self.data)

    def __ls(self, ls_func, path_transform):
        for wd, paths in czip(
            (self.local_wd, self.hdfs_wd), (self.local_paths, self.hdfs_paths)
        ):
            for p in paths:
                hdfs.dump(self.data, p, mode="wb")
            test_dir = "%s/%s" % (wd, "test_dir")
            test_path = "%s/%s" % (test_dir, "test_path")
            hdfs.dump(self.data, test_path, mode="wb")
            paths.append(test_dir)
            for recursive in False, True:
                if recursive:
                    paths.append(test_path)
                dir_list = [
                    path_transform(p) for p in ls_func(wd, recursive=recursive)
                ]
                self.assertEqual(sorted(dir_list), sorted(paths))

    def lsl(self):
        self.__ls(hdfs.lsl, lambda x: x["name"])

    def ls(self):
        self.__ls(hdfs.ls, lambda x: x)

    def mkdir(self):
        for wd in self.local_wd, self.hdfs_wd:
            d1 = "%s/d1" % wd
            d2 = "%s/d2" % d1
            hdfs.mkdir(d2)
            dir_list = hdfs.ls(d1)
            self.assertEqual(len(dir_list), 1)
            self.assertTrue(dir_list[0].endswith(d2))

    def load(self):
        for test_path in self.hdfs_paths[0], self.local_paths[0]:
            hdfs.dump(self.data, test_path, mode="wb")
            rdata = hdfs.load(test_path)
            self.assertEqual(rdata, self.data)

    def __make_tree(self, wd, root="d1", create=True):
        """
        d1
        |-- d2
        |   `-- f2
        `-- f1
        """
        d1 = "%s/%s" % (wd, root)
        t1 = FSTree(d1)
        d2 = "%s/d2" % d1
        t2 = t1.add(d2)
        if create:
            hdfs.mkdir(d2)
        for t, d, bn in ((t1, d1, "f1"), (t2, d2, "f2")):
            f = "%s/%s" % (d, bn)
            if create:
                hdfs.dump(self.data, f, mode="wb")
            t.add(f, 0)
        return t1

    def __cp_file(self, wd):
        fn = "%s/fn" % wd
        hdfs.dump(self.data, fn, mode="wb")
        dest_dir = "%s/dest_dir" % wd
        hdfs.mkdir(dest_dir)
        fn_copy_on_wd = "%s/fn_copy" % wd
        hdfs.cp(fn, fn_copy_on_wd, mode="wb")
        self.assertEqual(hdfs.load(fn_copy_on_wd), self.data)
        self.assertRaises(IOError, hdfs.cp, fn, fn_copy_on_wd)
        fn_copy_on_dest_dir = "%s/fn" % dest_dir
        hdfs.cp(fn, dest_dir, mode="wb")
        self.assertEqual(hdfs.load(fn_copy_on_dest_dir), self.data)
        self.assertRaises(IOError, hdfs.cp, fn, dest_dir)

    def __cp_dir(self, wd):
        src_dir = "%s/src_dir" % wd
        hdfs.mkdir(src_dir)
        copy_on_wd = "%s/src_dir_copy" % wd
        copy_on_copy_on_wd = "%s/src_dir" % copy_on_wd
        hdfs.cp(src_dir, copy_on_wd, mode="wb")
        self.assertTrue(hdfs.path.exists(copy_on_wd))
        hdfs.cp(src_dir, copy_on_wd, mode="wb")
        self.assertTrue(hdfs.path.exists(copy_on_copy_on_wd))
        self.assertRaises(IOError, hdfs.cp, src_dir, copy_on_wd)

    def __cp_recursive(self, wd):
        src_t = self.__make_tree(wd)
        src = src_t.name
        copy_on_wd = "%s_copy" % src
        src_bn, copy_on_wd_bn = [
            hdfs.path.basename(d) for d in (src, copy_on_wd)
        ]
        hdfs.cp(src, copy_on_wd, mode="wb")
        exp_t = self.__make_tree(wd, root=copy_on_wd_bn, create=False)
        for t, exp_t in czip(src_t.walk(), exp_t.walk()):
            self.assertTrue(hdfs.path.exists(exp_t.name))
            if t.kind == 0:
                self.assertEqual(hdfs.load(exp_t.name), self.data)
        # check semantics when target dir already exists
        hdfs.rmr(copy_on_wd)
        hdfs.mkdir(copy_on_wd)
        hdfs.cp(src, copy_on_wd, mode="wb")
        exp_t = self.__make_tree(copy_on_wd, root=src_bn, create=False)
        for t, exp_t in czip(src_t.walk(), exp_t.walk()):
            self.assertTrue(hdfs.path.exists(exp_t.name))
            if t.kind == 0:
                self.assertEqual(hdfs.load(exp_t.name), self.data)

    def cp(self):
        for wd in self.local_wd, self.hdfs_wd:
            self.__cp_file(wd)
            self.__cp_dir(wd)
            self.__cp_recursive(wd)

    def put(self):
        src = hdfs.path.split(self.local_paths[0])[-1]
        dest = self.hdfs_paths[0]
        with open(src, "wb") as f:
            f.write(self.data)
        hdfs.put(src, dest, mode="wb")
        with hdfs.open(dest) as fi:
            rdata = fi.read()
        self.assertEqual(rdata, self.data)

    def get(self):
        src = self.hdfs_paths[0]
        dest = hdfs.path.split(self.local_paths[0])[-1]
        hdfs.dump(self.data, src, mode="wb")
        hdfs.get(src, dest, mode="wb")
        with open(dest, 'rb') as fi:
            rdata = fi.read()
        self.assertEqual(rdata, self.data)

    def rmr(self):
        for wd in self.local_wd, self.hdfs_wd:
            t1 = self.__make_tree(wd)
            hdfs.rmr(t1.name)
            self.assertEqual(len(hdfs.ls(wd)), 0)

    def chmod(self):
        with tempfile.NamedTemporaryFile(suffix='_%s' % UNI_CHR) as f:
            hdfs.chmod("file://" + f.name, 444)
            s = os.stat(f.name)
            self.assertEqual(444, stat.S_IMODE(s.st_mode))

    def move(self):
        for wd in self.local_wd, self.hdfs_wd:
            t1 = self.__make_tree(wd)
            t2 = [_ for _ in t1.children if _.kind == 1][0]
            f2 = t2.children[0]
            hdfs.move(f2.name, t1.name)
            ls = [os.path.basename(_) for _ in hdfs.ls(t1.name)]
            self.assertTrue(os.path.basename(f2.name) in ls)
            self.assertEqual(len(hdfs.ls(t2.name)), 0)

    def chown(self):
        new_user = 'nobody'
        test_path = self.hdfs_paths[0]
        hdfs.dump(self.data, test_path, mode="wb")
        hdfs.chown(test_path, user=new_user)
        path_info = hdfs.lsl(test_path)[0]
        self.assertEqual(path_info['owner'], new_user)
        prev_owner = path_info['owner']
        prev_grp = path_info['group']
        # owner and group should remain unchanged
        hdfs.chown(test_path, user='', group='')
        path_info = hdfs.lsl(test_path)[0]
        self.assertEqual(path_info['owner'], prev_owner)
        self.assertEqual(path_info['group'], prev_grp)

    def rename(self):
        test_path = self.hdfs_paths[0]
        new_path = "%s.new" % test_path
        hdfs.dump(self.data, test_path, mode="wb")
        hdfs.rename(test_path, new_path)
        self.assertFalse(hdfs.path.exists(test_path))
        self.assertTrue(hdfs.path.exists(new_path))
        self.assertRaises(
            RuntimeError, hdfs.rename, test_path, self.local_paths[0]
        )

    def renames(self):
        test_path = self.hdfs_paths[0]
        hdfs.dump(self.data, test_path, mode="wb")
        new_d = hdfs.path.join(self.hdfs_wd, "new_dir")
        new_path = hdfs.path.join(new_d, "new_p")
        hdfs.renames(test_path, new_path)
        self.assertFalse(hdfs.path.exists(test_path))
        self.assertTrue(hdfs.path.exists(new_path))

    def capacity(self):
        fs = hdfs.hdfs("", 0)
        self.assertRaises(RuntimeError, fs.capacity)
        fs.close()
        if not hdfs.default_is_local():
            fs = hdfs.hdfs("default", 0)
            cap = fs.capacity()
            self.assertGreaterEqual(cap, 0)

    def get_hosts(self):
        if hdfs.default_is_local():
            # only run on HDFS
            return
        hdfs.dump(self.data, self.hdfs_paths[0], mode="wb")
        fs = hdfs.hdfs("default", 0)
        hs = fs.get_hosts(self.hdfs_paths[0], 0, 10)
        self.assertTrue(len(hs) > 0)
        self.assertRaises(
            ValueError, fs.get_hosts, self.hdfs_paths[0], -10, 10
        )
        self.assertRaises(ValueError, fs.get_hosts, self.hdfs_paths[0], 0, -10)

    def thread_allow(self):
        # test whether our code is properly allowing other python threads to
        # make progress while we're busy doing I/O
        class BusyCounter(Thread):
            def __init__(self):
                super(BusyCounter, self).__init__()
                self.done = False
                self._count = 0

            @property
            def count(self):
                return self._count

            def run(self):
                while not self.done:
                    self._count += 1

        class BusyContext(object):
            def __init__(self):
                self.counter = None

            def __enter__(self):
                self.counter = BusyCounter()
                self.counter.start()

            def __exit__(self, _1, _2, _3):
                self.counter.done = True
                self.counter.join()

            @property
            def count(self):
                return self.counter.count

        some_data = b"a" * (5 * 1024 * 1024)  # 5 MB
        counter = BusyContext()

        ###########################
        acceptable_threshold = 5
        # The tests were sometimes failing on TravisCI (slower machines) with
        # counts below 100.  A test where we left the GIL locked showed that in
        # that case counter value doesn't change at all across calls, so in
        # theory even an increment of 1 would demonstrate that the mechanism is
        # working.

        # If the hdfs call doesn't release the GIL, the counter won't make any
        # progress during the HDFS call and will be stuck at 0.  On the other
        # hand, if the GIL is release during the operation we'll see a count
        # value > 0.
        fs = hdfs.hdfs("default", 0)
        with fs.open_file(self.hdfs_paths[0], "w") as f:
            with counter:
                f.write(some_data)
            self.assertGreaterEqual(counter.count, acceptable_threshold)

        with fs.open_file(self.hdfs_paths[0], "r") as f:
            with counter:
                f.read()
            self.assertGreaterEqual(counter.count, acceptable_threshold)

        with counter:
            fs.get_hosts(self.hdfs_paths[0], 0, 10)
        self.assertGreaterEqual(counter.count, acceptable_threshold)

        with counter:
            fs.list_directory('/')
        self.assertGreaterEqual(counter.count, acceptable_threshold)

        with counter:
            hdfs.cp(self.hdfs_paths[0], self.hdfs_paths[0] + '_2', mode="wb")
        self.assertGreaterEqual(counter.count, acceptable_threshold)

        with counter:
            hdfs.rmr(self.hdfs_paths[0] + '_2')
        self.assertGreaterEqual(counter.count, acceptable_threshold)

        # ...we could go on, but the better strategy would be to insert a check
        # analogous to these in each method's unit test


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestHDFS("open"))
    suite_.addTest(TestHDFS("dump"))
    suite_.addTest(TestHDFS("lsl"))
    suite_.addTest(TestHDFS("ls"))
    suite_.addTest(TestHDFS("mkdir"))
    suite_.addTest(TestHDFS("load"))
    suite_.addTest(TestHDFS("cp"))
    suite_.addTest(TestHDFS("put"))
    suite_.addTest(TestHDFS("get"))
    suite_.addTest(TestHDFS("rmr"))
    suite_.addTest(TestHDFS("chmod"))
    suite_.addTest(TestHDFS("move"))
    suite_.addTest(TestHDFS("chown"))
    suite_.addTest(TestHDFS("rename"))
    suite_.addTest(TestHDFS("renames"))
    suite_.addTest(TestHDFS("capacity"))
    suite_.addTest(TestHDFS("get_hosts"))
    suite_.addTest(TestHDFS("thread_allow"))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
