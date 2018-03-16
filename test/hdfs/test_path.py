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

import sys
import os
import unittest
import tempfile
from numbers import Number

import pydoop.hdfs as hdfs
from pydoop.hdfs.common import DEFAULT_PORT, DEFAULT_USER
from pydoop.utils.misc import make_random_str
from pydoop.test_utils import UNI_CHR


def uni_last(tup):
    return tup[:-1] + (tup[-1] + UNI_CHR,)


class TestSplit(unittest.TestCase):

    def good(self):
        cases = [
            ('hdfs://localhost:9000/', ('localhost', 9000, '/')),
            ('hdfs://localhost:9000/a/b', ('localhost', 9000, '/a/b')),
            ('hdfs://localhost/a/b', ('localhost', DEFAULT_PORT, '/a/b')),
            ('hdfs:///a/b', ('default', 0, '/a/b')),
            ('hdfs:/', ('default', 0, '/')),
            ('file:///a/b', ('', 0, '/a/b')),
            ('file:/a/b', ('', 0, '/a/b')),
            ('file:///a', ('', 0, '/a')),
            ('file:/a', ('', 0, '/a')),
            ('file://temp/foo.txt', ('', 0, 'temp/foo.txt')),
            ('file://temp', ('', 0, 'temp')),
        ]
        if hdfs.default_is_local():
            cases.extend([
                ('///a/b', ('', 0, '/a/b')),
                ('/a/b', ('', 0, '/a/b')),
                ('a/b', ('', 0, 'a/b')),
            ])
        else:
            cases.extend([
                ('///a/b', ('default', 0, '/a/b')),
                ('/a/b', ('default', 0, '/a/b')),
                ('a/b', ('default', 0, '/user/%s/a/b' % DEFAULT_USER)),
            ])
        for p, r in cases:
            self.assertEqual(hdfs.path.split(p), r)
        for p, r in cases[1:]:
            self.assertEqual(hdfs.path.split(p + UNI_CHR), uni_last(r))

    def good_with_user(self):
        if hdfs.default_is_local():
            cases = [
                ('a/b', u, ('', 0, 'a/b')) for u in [None, DEFAULT_USER, 'foo']
            ]
        else:
            cases = [
                ('a/b', None, ('default', 0, '/user/%s/a/b' % DEFAULT_USER)),
                ('a/b', DEFAULT_USER, (
                    'default', 0, '/user/%s/a/b' % DEFAULT_USER
                )),
                ('a/b', 'foo', ('default', 0, '/user/foo/a/b')),
            ]
        for p, u, r in cases:
            self.assertEqual(hdfs.path.split(p, u), r)
            self.assertEqual(hdfs.path.split(p + UNI_CHR, u), uni_last(r))

    def bad(self):
        cases = [
            '',                                  # not allowed in the Java API
            'hdfs:',                             # no scheme-specific part
            'hdfs://',                           # path part is empty
            'ftp://localhost:9000/',             # bad scheme
            'hdfs://localhost:spam/',            # port is not an int
            'hdfs://localhost:9000',             # path part is empty
            'hdfs://localhost:9000/a:b',         # colon outside netloc
            '//localhost:9000/a/b',              # null scheme
        ]
        if not hdfs.default_is_local():
            cases.append('/localhost:9000/a/b')  # colon outside netloc
        for p in cases:
            self.assertRaises(ValueError, hdfs.path.split, p)

    def splitext(self):
        for pre in '', 'file:', 'hdfs://host:1':
            name, ext = '%sfoo' % pre, '.txt'
            self.assertEqual(hdfs.path.splitext(name + ext), (name, ext))
        p = 'hdfs://foo.com:1/'
        self.assertEqual(hdfs.path.splitext(p), (p, ''))


class TestUnparse(unittest.TestCase):

    def good(self):
        cases = [
            (('hdfs', 'host:1', '/'), 'hdfs://host:1/'),
            (('file', '', '/'), 'file:/'),
            (('hdfs', 'host:1', UNI_CHR), 'hdfs://host:1/%s' % UNI_CHR),
            (('file', '', UNI_CHR), 'file:/%s' % UNI_CHR),
            (('', '', UNI_CHR), UNI_CHR),
        ]
        for (scheme, netloc, path), exp_uri in cases:
            self.assertEqual(hdfs.path.unparse(scheme, netloc, path), exp_uri)

    def bad(self):
        self.assertRaises(ValueError, hdfs.path.unparse, '', 'host:1', '/a')


class TestJoin(unittest.TestCase):

    def __check_join(self, cases):
        for p, r in cases:
            self.assertEqual(hdfs.path.join(*p), r)

    def simple(self):
        self.__check_join([
            (('foo', 'bar', 'tar'), 'foo/bar/tar'),
            (('/foo', 'bar', 'tar'), '/foo/bar/tar'),
        ])

    def slashes(self):
        self.__check_join([
            (('foo/', 'bar/', 'tar'), 'foo/bar/tar'),
            (('/foo/', 'bar/', 'tar'), '/foo/bar/tar'),
        ])

    def absolute(self):
        self.__check_join([
            (('foo', '/bar', 'tar'), '/bar/tar'),
            (('foo', 'hdfs://host:1/bar', 'tar'), 'hdfs://host:1/bar/tar'),
            (('foo', 'file:/bar', 'tar'), 'file:/bar/tar'),
            (('foo', 'file:///bar', 'tar'), 'file:///bar/tar'),
        ])

    def full(self):
        self.__check_join([
            (('hdfs://host:1/', '/foo'), 'hdfs://host:1/foo'),
            (('hdfs://host:1/', 'file:/foo', '/bar'), 'file:/foo/bar'),
            (('foo', '/bar', 'hdfs://host:1/tar'), 'hdfs://host:1/tar'),
        ])

    def unicode_(self):
        self.__check_join(
            [(('/foo', 'bar', UNI_CHR), '/foo/bar/%s' % UNI_CHR)]
        )


class TestAbspath(unittest.TestCase):

    def setUp(self):
        if hdfs.default_is_local():
            self.root = "file:"
        else:
            fs = hdfs.hdfs("default", 0)
            self.root = "hdfs://%s:%s" % (fs.host, fs.port)
            fs.close()
        self.p = 'a/%s' % UNI_CHR

    def without_user(self):
        abs_p = hdfs.path.abspath(self.p, user=None, local=False)
        if hdfs.default_is_local():
            self.assertEqual(
                abs_p, '%s%s' % (self.root, os.path.abspath(self.p))
            )
        else:
            self.assertEqual(
                abs_p, '%s/user/%s/%s' % (self.root, DEFAULT_USER, self.p)
            )

    def with_user(self):
        abs_p = hdfs.path.abspath(self.p, user="pydoop", local=False)
        if hdfs.default_is_local():
            self.assertEqual(
                abs_p, '%s%s' % (self.root, os.path.abspath(self.p))
            )
        else:
            self.assertEqual(abs_p, '%s/user/pydoop/%s' % (self.root, self.p))

    def forced_local(self):
        for user in None, "pydoop":
            abs_p = hdfs.path.abspath(self.p, user=user, local=True)
            self.assertEqual(abs_p, 'file:%s' % os.path.abspath(self.p))

    def already_absolute(self):
        for p in ('file:/a/%s' % UNI_CHR,
                  'hdfs://localhost:9000/a/%s' % UNI_CHR):
            for user in None, "pydoop":
                abs_p = hdfs.path.abspath(p, user=user, local=False)
                self.assertEqual(abs_p, p)
                abs_p = hdfs.path.abspath(p, user=user, local=True)
                self.assertEqual(abs_p, 'file:%s' % os.path.abspath(p))


class TestSplitBasenameDirname(unittest.TestCase):

    def runTest(self):
        cases = [  # path, expected dirname, expected basename
            ("hdfs://host:1/a/%s" % UNI_CHR, "hdfs://host:1/a", UNI_CHR),
            ("hdfs://host:1/", "hdfs://host:1/", ""),
            ("hdfs:/", "hdfs:/", ""),
            ("file:/", "file:/", ""),
            ("a/%s" % UNI_CHR, "a", UNI_CHR),
            ("/a/%s" % UNI_CHR, "/a", UNI_CHR),
            (UNI_CHR, "", UNI_CHR),
            ('/%s' % UNI_CHR, "/", UNI_CHR),
            ('', '', ''),
        ]
        for p, d, bn in cases:
            self.assertEqual(hdfs.path.dirname(p), d)
            self.assertEqual(hdfs.path.basename(p), bn)
            self.assertEqual(hdfs.path.splitpath(p), (d, bn))


class TestExists(unittest.TestCase):

    def good(self):
        base_path = make_random_str()
        for path in base_path, base_path + UNI_CHR:
            hdfs.dump("foo\n", path)
            self.assertTrue(hdfs.path.exists(path))
            hdfs.rmr(path)
            self.assertFalse(hdfs.path.exists(path))


class TestKind(unittest.TestCase):

    def setUp(self):
        self.path = make_random_str()
        self.u_path = self.path + UNI_CHR

    def test_kind(self):
        for path in self.path, self.u_path:
            self.assertTrue(hdfs.path.kind(path) is None)
            try:
                hdfs.dump("foo\n", path)
                self.assertEqual('file', hdfs.path.kind(path))
                hdfs.rmr(path)
                hdfs.mkdir(path)
                self.assertEqual('directory', hdfs.path.kind(path))
            finally:
                try:
                    hdfs.rmr(path)
                except IOError:
                    pass

    def test_isfile(self):
        for path in self.path, self.u_path:
            self.assertFalse(hdfs.path.isfile(path))
            try:
                hdfs.dump("foo\n", path)
                self.assertTrue(hdfs.path.isfile(path))
                hdfs.rmr(path)
                hdfs.mkdir(path)
                self.assertFalse(hdfs.path.isfile(path))
            finally:
                try:
                    hdfs.rmr(path)
                except IOError:
                    pass

    def test_isdir(self):
        for path in self.path, self.u_path:
            self.assertFalse(hdfs.path.isdir(path))
            try:
                hdfs.dump("foo\n", path)
                self.assertFalse(hdfs.path.isdir(path))
                hdfs.rmr(path)
                hdfs.mkdir(path)
                self.assertTrue(hdfs.path.isdir(path))
            finally:
                try:
                    hdfs.rmr(path)
                except IOError:
                    pass


class TestExpand(unittest.TestCase):

    def expanduser(self):
        for pre in '~', '~%s' % DEFAULT_USER:
            for rest in '', '/d':
                p = '%s%s' % (pre, rest)
                if hdfs.default_is_local():
                    self.assertEqual(
                        hdfs.path.expanduser(p), os.path.expanduser(p)
                    )
                else:
                    exp_res = '/user/%s%s' % (DEFAULT_USER, rest)
                    self.assertEqual(hdfs.path.expanduser(p), exp_res)

    def expanduser_no_expansion(self):
        for pre in ('hdfs://host:1', 'file://', ''):
            for rest in ('/~', '/~foo', '/d/~', '/d/~foo'):
                p = '%s%s' % (pre, rest)
                self.assertEqual(hdfs.path.expanduser(p), p)

    def expandvars(self):
        k, v = 'PYDOOP_TEST_K', 'PYDOOP_TEST_V'
        p = 'hdfs://host:1/${%s}' % k
        os.environ[k] = v
        exp_res = '%s/%s' % (p.rsplit('/', 1)[0], v)
        try:
            self.assertEqual(hdfs.path.expandvars(p), exp_res)
        finally:
            del os.environ[k]


class TestStat(unittest.TestCase):

    NMAP = {
        'st_mode': 'permissions',
        'st_uid': 'owner',
        'st_gid': 'group',
        'st_size': 'size',
        'st_atime': 'last_access',
        'st_mtime': 'last_mod',
        'st_blksize': 'block_size',
    }

    def stat(self):
        if hdfs.default_is_local():
            return
        bn = '%s%s' % (make_random_str(), UNI_CHR)
        fn = '/user/%s/%s' % (DEFAULT_USER, bn)
        fs = hdfs.hdfs("default", 0)
        p = "hdfs://%s:%s%s" % (fs.host, fs.port, fn)
        with fs.open_file(fn, 'wt') as fo:
            fo.write(make_random_str())
        info = fs.get_path_info(fn)
        fs.close()
        s = hdfs.path.stat(p)
        for n1, n2 in self.NMAP.items():
            attr = getattr(s, n1, None)
            self.assertFalse(attr is None)
            self.assertEqual(attr, info[n2])
        self.__check_extra_args(s, info)
        self.__check_wrapper_funcs(p)
        hdfs.rmr(p)

    def stat_on_local(self):
        wd_ = tempfile.mkdtemp(prefix='pydoop_', suffix=UNI_CHR)
        p_ = os.path.join(wd_, make_random_str())
        if hdfs.default_is_local():
            wd, p = wd_, p_
            host = "default"
        else:
            wd, p = ('file:%s' % _ for _ in (wd_, p_))
            host = ""
        fs = hdfs.hdfs(host, 0)
        with fs.open_file(p_, 'w') as fo:
            fo.write(b"foobar\n")
        info = fs.get_path_info(p_)
        fs.close()
        s = hdfs.path.stat(p)
        os_s = os.stat(p_)
        for n in dir(s):
            if n.startswith('st_'):
                try:
                    exp_v = getattr(os_s, n)
                except AttributeError:
                    try:
                        exp_v = info[self.NMAP[n]]
                    except KeyError:
                        continue
                    self.assertEqual(getattr(s, n), exp_v)
        self.__check_extra_args(s, info)
        self.__check_wrapper_funcs(p)
        hdfs.rmr(wd)

    def stat_on_dir(self):
        s = hdfs.path.stat('/user/%s' % DEFAULT_USER)
        for attr in s.st_size, s.st_blksize, s.st_blocks:
            self.assertEqual(attr, 0)

    def __check_extra_args(self, stat_res, path_info):
        for n in 'kind', 'name', 'replication':
            attr = getattr(stat_res, '%s' % n, None)
            self.assertFalse(attr is None)
            self.assertEqual(attr, path_info[n])

    def __check_wrapper_funcs(self, path):
        for n in 'getatime', 'getmtime', 'getctime', 'getsize':
            func = getattr(hdfs.path, n)
            self.assertTrue(isinstance(func(path), Number))


class TestIsSomething(unittest.TestCase):

    def full_and_abs(self):
        for name in 'isfull', 'isabs':
            test = getattr(hdfs.path, name)
            for p in 'hdfs://host:1/foo', 'file:/foo':
                self.assertTrue(test(p))
            self.assertFalse(test('foo'))
        self.assertFalse(hdfs.path.isfull('/foo'))
        self.assertTrue(hdfs.path.isabs('/foo'))

    def islink(self):
        wd_ = tempfile.mkdtemp(prefix='pydoop_', suffix=UNI_CHR)
        wd = 'file:%s' % wd_
        self.assertFalse(hdfs.path.islink(wd))
        link = os.path.join(wd_, make_random_str())
        os.symlink(wd_, link)
        self.assertTrue(hdfs.path.islink('file:%s' % link))
        hdfs.rmr(wd)

    def ismount(self):
        self.assertFalse(hdfs.path.ismount('hdfs://host:1/foo'))


class TestNorm(unittest.TestCase):

    def normpath(self):
        for pre in '', 'file:', 'hdfs://host:1':
            post = '/a/./b/c/../../foo'
            npost = '/a/foo'
            self.assertEqual(hdfs.path.normpath(pre + post), pre + npost)
            self.assertEqual(hdfs.path.normpath('a/./b/c/../../foo'), 'a/foo')


class TestReal(unittest.TestCase):

    def realpath(self):
        wd_ = tempfile.mkdtemp(prefix='pydoop_', suffix=UNI_CHR)
        wd = 'file:%s' % wd_
        link = os.path.join(wd_, make_random_str())
        os.symlink(wd_, link)
        expected_path = ('file:%s%s' % ("/private", wd_)
                         if sys.platform == "darwin"
                         else 'file:%s' % wd_)
        self.assertEqual(hdfs.path.realpath('file:%s' % link), expected_path)
        hdfs.rmr(wd)


class TestSame(unittest.TestCase):

    def samefile_link(self):
        wd_ = tempfile.mkdtemp(prefix='pydoop_', suffix=UNI_CHR)
        wd = 'file:%s' % wd_
        link = os.path.join(wd_, make_random_str())
        os.symlink(wd_, link)
        self.assertTrue(hdfs.path.samefile('file:%s' % link, 'file:%s' % wd_))
        hdfs.rmr(wd)

    def samefile_rel(self):
        p = make_random_str() + UNI_CHR
        hdfs.dump("foo\n", p)
        self.assertTrue(hdfs.path.samefile(p, hdfs.path.abspath(p)))
        hdfs.rmr(p)

    def samefile_norm(self):
        for pre in '', 'file:/', 'hdfs://host:1/':
            self.assertTrue(hdfs.path.samefile(pre + 'a/b/../c', pre + 'a/c'))

    def samefile_user(self):
        if not hdfs.default_is_local():
            self.assertTrue(hdfs.path.samefile('fn', '/user/u/fn', user='u'))


class TestAccess(unittest.TestCase):

    def setUp(self):
        self.path = make_random_str() + UNI_CHR
        hdfs.mkdir(self.path)

    def tearDown(self):
        hdfs.rmr(self.path)

    # FIXME: far from exhaustive.  This is a slow test
    def __test(self, offset, user=None):
        for mode in os.R_OK, os.W_OK, os.X_OK:
            hdfs.chmod(self.path, mode << offset)
            print(' * mode now: %03o' % hdfs.path.stat(self.path).st_mode)
            self.assertTrue(hdfs.path.access(self.path, mode, user=user))

    def test_owner(self):
        self.__test(6)

    def test_other(self):
        self.__test(0, user=make_random_str())


class TestUtime(unittest.TestCase):

    def runTest(self):
        path = make_random_str() + UNI_CHR
        hdfs.dump("foo\n", path)
        st = hdfs.path.stat(path)
        atime, mtime = [getattr(st, 'st_%stime' % _) for _ in 'am']
        new_atime, new_mtime = atime + 100, mtime + 200
        hdfs.path.utime(path, (new_atime, new_mtime))
        st = hdfs.path.stat(path)
        self.assertEqual(st.st_atime, new_atime)
        self.assertEqual(st.st_mtime, new_mtime)
        hdfs.rmr(path)


class TestCallFromHdfs(unittest.TestCase):

    def setUp(self):
        self.path = make_random_str() + UNI_CHR
        hdfs.dump("foo\n", self.path)

    def tearDown(self):
        hdfs.rmr(self.path)

    def test_stat(self):
        for name in 'stat', 'lstat':
            self.assertTrue(hasattr(hdfs, name))
            func = getattr(hdfs, name)
            func(self.path)
            func(self.path, user=DEFAULT_USER)

    def test_access(self):
        self.assertTrue(hasattr(hdfs, 'access'))
        hdfs.access(self.path, os.F_OK)
        hdfs.access(self.path, os.F_OK, user=DEFAULT_USER)

    def test_utime(self):
        self.assertTrue(hasattr(hdfs, 'utime'))
        hdfs.utime(self.path)
        hdfs.utime(self.path, times=(1e9, 1e9))
        hdfs.utime(self.path, times=(1e9, 1e9), user=DEFAULT_USER)


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestSplit('good'))
    suite_.addTest(TestSplit('good_with_user'))
    suite_.addTest(TestSplit('bad'))
    suite_.addTest(TestSplit('splitext'))
    suite_.addTest(TestUnparse('good'))
    suite_.addTest(TestUnparse('bad'))
    suite_.addTest(TestJoin('simple'))
    suite_.addTest(TestJoin('slashes'))
    suite_.addTest(TestJoin('absolute'))
    suite_.addTest(TestJoin('full'))
    suite_.addTest(TestJoin('unicode_'))
    suite_.addTest(TestAbspath('with_user'))
    suite_.addTest(TestAbspath('without_user'))
    suite_.addTest(TestAbspath('forced_local'))
    suite_.addTest(TestAbspath('already_absolute'))
    suite_.addTest(TestSplitBasenameDirname('runTest'))
    suite_.addTest(TestExists('good'))
    suite_.addTest(TestExpand('expanduser'))
    suite_.addTest(TestExpand('expanduser_no_expansion'))
    suite_.addTest(TestExpand('expandvars'))
    suite_.addTest(TestStat('stat'))
    suite_.addTest(TestStat('stat_on_local'))
    suite_.addTest(TestStat('stat_on_dir'))
    suite_.addTest(TestIsSomething('full_and_abs'))
    suite_.addTest(TestIsSomething('islink'))
    suite_.addTest(TestIsSomething('ismount'))
    suite_.addTest(TestNorm('normpath'))
    suite_.addTest(TestReal('realpath'))
    suite_.addTest(TestSame('samefile_link'))
    suite_.addTest(TestSame('samefile_rel'))
    suite_.addTest(TestSame('samefile_norm'))
    suite_.addTest(TestSame('samefile_user'))
    suite_.addTest(TestAccess('test_owner'))
    suite_.addTest(TestAccess('test_other'))
    suite_.addTest(TestUtime('runTest'))
    suite_.addTest(TestCallFromHdfs('test_stat'))
    suite_.addTest(TestCallFromHdfs('test_access'))
    suite_.addTest(TestCallFromHdfs('test_utime'))
    suite_.addTest(unittest.TestLoader().loadTestsFromTestCase(TestKind))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
