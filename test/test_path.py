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

import os, unittest

import pydoop.hdfs as hdfs
from pydoop.hdfs.common import DEFAULT_PORT, DEFAULT_USER
import pydoop.utils as utils


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

  def bad(self):
    cases = [
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
    for p, r in [
      (('/foo', 'bar', 'tar'), '/foo/bar/tar'),
      (('/foo/', 'bar/', 'tar/'), '/foo/bar/tar'),
      (('/foo/', 'hdfs://host:9000/bar/', 'tar/'), 'hdfs://host:9000/bar/tar'),
      (('/foo/', 'file:/bar/', 'tar/'), 'file:/bar/tar'),
      (('/foo/', 'file:///bar/', 'tar/'), 'file:///bar/tar'),
      ]:
      self.assertEqual(hdfs.path.join(*p), r)


class TestAbspath(unittest.TestCase):

  def setUp(self):
    if hdfs.default_is_local():
      self.root = "file:"
    else:
      fs = hdfs.hdfs("default", 0)
      self.root = "hdfs://%s:%s" % (fs.host, fs.port)
      fs.close()

  def without_user(self):
    p = 'foo/bar'
    abs_p = hdfs.path.abspath(p, user=None, local=False)
    if hdfs.default_is_local():
      self.assertEqual(abs_p, '%s%s' % (self.root, os.path.abspath(p)))
    else:
      self.assertEqual(abs_p, '%s/user/%s/%s' % (self.root, DEFAULT_USER, p))

  def with_user(self):
    p = 'foo/bar'
    abs_p = hdfs.path.abspath(p, user="pydoop", local=False)
    if hdfs.default_is_local():
      self.assertEqual(abs_p, '%s%s' % (self.root, os.path.abspath(p)))
    else:
      self.assertEqual(abs_p, '%s/user/pydoop/%s' % (self.root, p))

  def forced_local(self):
    p = 'foo/bar'
    for user in None, "pydoop":
      abs_p = hdfs.path.abspath(p, user=user, local=True)
      self.assertEqual(abs_p, 'file:%s' % os.path.abspath(p))

  def already_absolute(self):
    for p in 'file:/foo/bar', 'hdfs://localhost:9000/foo/bar':
      for user in None, "pydoop":
        abs_p = hdfs.path.abspath(p, user=user, local=False)
        self.assertEqual(abs_p, p)
        abs_p = hdfs.path.abspath(p, user=user, local=True)
        self.assertEqual(abs_p, 'file:%s' % os.path.abspath(p))


class TestBasename(unittest.TestCase):

  def good(self):
    self.assertEqual(hdfs.path.basename("hdfs://localhost:9000/foo/bar"), "bar")


class TestExists(unittest.TestCase):

  def good(self):
    path = utils.make_random_str()
    hdfs.dump("foo\n", path)
    self.assertTrue(hdfs.path.exists(path))
    hdfs.rmr(path)
    self.assertFalse(hdfs.path.exists(path))


class TestKind(unittest.TestCase):

  def test_kind(self):
    path = utils.make_random_str()
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
    path = utils.make_random_str()
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
    path = utils.make_random_str()
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
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestKind))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
