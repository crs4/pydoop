# BEGIN_COPYRIGHT
# 
# Copyright 2009-2014 CRS4.
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

# pylint: disable=W0311

import os, unittest, tempfile
from numbers import Number

import pydoop.hdfs as hdfs
from pydoop.hdfs.common import DEFAULT_PORT, DEFAULT_USER
from pydoop.utils import make_random_str

from utils import UNI_CHR


def uni_last(tup):
  return tup[:-1] + (tup[-1]+UNI_CHR,)


class TestSplit(unittest.TestCase):

  def good(self):
    cases = [
      ('hdfs://localhost:9000/', ('localhost', 9000, '/')),
      ('hdfs://localhost:9000/a/b', ('localhost', 9000, '/a/b')),
      ('hdfs://localhost/a/b', ('localhost', DEFAULT_PORT, '/a/b')),
      ('hdfs:///a/b', ('default', 0, '/a/b')),
      ('file:///a/b', ('', 0, '/a/b')),
      ('file:/a/b', ('', 0, '/a/b')),
      ('file:///a', ('', 0, '/a')),
      ('file:/a', ('', 0, '/a')),
      ('file://localhost:9000/a/b', ('', 0, '/localhost:9000/a/b')),
      ]
    if hdfs.default_is_local():
      cases.extend([
        ('//localhost:9000/a/b', ('', 0, '/localhost:9000/a/b')),
        ('/a/b', ('', 0, '/a/b')),
        ('a/b', ('', 0, 'a/b')),
        ])
    else:
      cases.extend([
        ('//localhost:9000/a/b', ('localhost', 9000, '/a/b')),
        ('/a/b', ('default', 0, '/a/b')),
        ('a/b', ('default', 0, '/user/%s/a/b' % DEFAULT_USER)),
        ])
    for p, r in cases:
      self.assertEqual(hdfs.path.split(p), r)
    for p, r in cases[1:]:
      self.assertEqual(hdfs.path.split(p+UNI_CHR), uni_last(r))

  def good_with_user(self):
    if hdfs.default_is_local():
      cases = [('a/b', u, ('', 0, 'a/b')) for u in None, DEFAULT_USER, 'foo']
    else:
      cases = [
        ('a/b', None, ('default', 0, '/user/%s/a/b' % DEFAULT_USER)),
        ('a/b', DEFAULT_USER, ('default', 0, '/user/%s/a/b' % DEFAULT_USER)),
        ('a/b', 'foo', ('default', 0, '/user/foo/a/b')),
        ]
    for p, u, r in cases:
      self.assertEqual(hdfs.path.split(p, u), r)
      self.assertEqual(hdfs.path.split(p+UNI_CHR, u), uni_last(r))

  def bad(self):
    cases = [
      '',                                  # not allowed in the Java API
      'ftp://localhost:9000/',             # bad scheme
      'hdfs://localhost:spam/',            # port is not an int
      'hdfs://localhost:9000',             # path part is empty
      'hdfs://localhost:9000/a:b',         # colon outside netloc
      ]
    if not hdfs.default_is_local():
      cases.append('/localhost:9000/a/b')  # colon outside netloc
    for p in cases:
      self.assertRaises(ValueError, hdfs.path.split, p)


class TestJoin(unittest.TestCase):

  def good(self):
    cases = [
      (('/foo', 'bar', 'tar'), '/foo/bar/tar'),
      (('/foo/', 'bar/', 'tar/'), '/foo/bar/tar'),
      (('/foo/', 'hdfs://host:9000/bar/', 'tar/'), 'hdfs://host:9000/bar/tar'),
      (('/foo/', 'file:/bar/', 'tar/'), 'file:/bar/tar'),
      (('/foo/', 'file:///bar/', 'tar/'), 'file:///bar/tar'),
      ]
    for p, r in cases:
      self.assertEqual(hdfs.path.join(*p), r)
    p, r = cases[0]
    self.assertEqual(hdfs.path.join(*uni_last(p)), r+UNI_CHR)


class TestAbspath(unittest.TestCase):

  def setUp(self):
    if hdfs.default_is_local():
      self.root = "file:"
    else:
      fs = hdfs.hdfs("default", 0)
      self.root = "hdfs://%s:%s" % (fs.host, fs.port)
      fs.close()
    self.p = 'foo/bar'
    self.u_p = self.p + UNI_CHR

  def without_user(self):
    for p in self.p, self.u_p:
      abs_p = hdfs.path.abspath(p, user=None, local=False)
      if hdfs.default_is_local():
        self.assertEqual(abs_p, '%s%s' % (self.root, os.path.abspath(p)))
      else:
        self.assertEqual(abs_p, '%s/user/%s/%s' % (self.root, DEFAULT_USER, p))

  def with_user(self):
    for p in self.p, self.u_p:
      abs_p = hdfs.path.abspath(p, user="pydoop", local=False)
      if hdfs.default_is_local():
        self.assertEqual(abs_p, '%s%s' % (self.root, os.path.abspath(p)))
      else:
        self.assertEqual(abs_p, '%s/user/pydoop/%s' % (self.root, p))

  def forced_local(self):
    for p in self.p, self.u_p:
      for user in None, "pydoop":
        abs_p = hdfs.path.abspath(p, user=user, local=True)
        self.assertEqual(abs_p, 'file:%s' % os.path.abspath(p))

  def already_absolute(self):
    for base_p in 'file:/foo/bar', 'hdfs://localhost:9000/foo/bar':
      for p in base_p, base_p + UNI_CHR:
        for user in None, "pydoop":
          abs_p = hdfs.path.abspath(p, user=user, local=False)
          self.assertEqual(abs_p, p)
          abs_p = hdfs.path.abspath(p, user=user, local=True)
          self.assertEqual(abs_p, 'file:%s' % os.path.abspath(p))


class TestBasename(unittest.TestCase):

  def good(self):
    p, bn = "hdfs://localhost:9000/foo/bar", "bar"
    self.assertEqual(hdfs.path.basename(p), bn)
    self.assertEqual(hdfs.path.basename(p+UNI_CHR), bn+UNI_CHR)


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
          self.assertEqual(hdfs.path.expanduser(p), os.path.expanduser(p))
        else:
          exp_res = '/user/%s%s' % (DEFAULT_USER, rest)
          self.assertEqual(hdfs.path.expanduser(p), exp_res)

  def expanduser_no_expansion(self):
    for pre in ('hdfs://host:1', 'file://', ''):
      for rest in ('/~', '/~foo', 'd/~', 'd/~foo'):
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
    bn = '%s%s' % (make_random_str(), UNI_CHR)
    fs = hdfs.hdfs("default", 0)
    p = "hdfs://%s:%s/user/%s/%s" % (fs.host, fs.port, DEFAULT_USER, bn)
    with fs.open_file(bn, 'w') as fo:
      fo.write(make_random_str())
    info = fs.get_path_info(bn)
    fs.close()
    s = hdfs.path.stat(p)
    for n1, n2 in self.NMAP.iteritems():
      attr = getattr(s, n1, None)
      self.assertFalse(attr is None)
      self.assertEqual(attr, info[n2])
    self.__check_extra_args(s, info)
    self.__check_wrapper_funcs(p)
    hdfs.rmr(p)

  def stat_on_local(self):
    wd_ = tempfile.mkdtemp(prefix='pydoop_', suffix=UNI_CHR)
    p_ = os.path.join(wd_, make_random_str())
    wd, p = ('file:%s' % _ for _ in (wd_, p_))
    fs = hdfs.hdfs("", 0)
    with fs.open_file(p_, 'w') as fo:
      fo.write(make_random_str())
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

  def isabs(self):
    for p in 'hdfs://host:1/foo', 'file:/foo':
      self.assertTrue(hdfs.path.isabs(p))
    self.assertFalse(hdfs.path.isabs('foo'))

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


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestSplit('good'))
  suite.addTest(TestSplit('good_with_user'))
  suite.addTest(TestSplit('bad'))
  suite.addTest(TestJoin('good'))
  suite.addTest(TestAbspath('with_user'))
  suite.addTest(TestAbspath('without_user'))
  suite.addTest(TestAbspath('forced_local'))
  suite.addTest(TestAbspath('already_absolute'))
  suite.addTest(TestBasename('good'))
  suite.addTest(TestExists('good'))
  suite.addTest(TestExpand('expanduser'))
  suite.addTest(TestExpand('expanduser_no_expansion'))
  suite.addTest(TestExpand('expandvars'))
  suite.addTest(TestStat('stat'))
  suite.addTest(TestStat('stat_on_local'))
  suite.addTest(TestIsSomething('isabs'))
  suite.addTest(TestIsSomething('islink'))
  suite.addTest(TestIsSomething('ismount'))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestKind))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
