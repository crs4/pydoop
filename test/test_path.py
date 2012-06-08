# BEGIN_COPYRIGHT
# 
# Copyright 2012 CRS4.
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

import os, unittest, uuid

import pydoop.hdfs as hdfs
from pydoop.hdfs.common import DEFAULT_PORT, DEFAULT_USER


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
    fs = hdfs.hdfs("default", 0)
    self.host = fs.host
    self.port = fs.port
    fs.close()
    self.root = "hdfs://%s:%s" % (self.host, self.port)

  def good(self):
    p = 'foo/bar'
    local_abs_p = 'file:%s' % os.path.abspath(p)
    for kw, r in [
      ({"user": None, "local": False},
       '%s/user/%s/%s' % (self.root, DEFAULT_USER, p)),
      ({"user": "pydoop", "local": False},
       '%s/user/pydoop/%s' % (self.root, p)),
      ({"user": None, "local": True}, local_abs_p),
      ]:
      if hdfs.default_is_local():
        self.assertEqual(hdfs.path.abspath(p, **kw), local_abs_p)
      else:
        self.assertEqual(hdfs.path.abspath(p, **kw), r)
    p = local_abs_p
    local_abs_p = 'file:%s' % os.path.abspath(p)
    for kw, r in [
      ({"user": None, "local": False}, p),
      ({"user": None, "local": True}, local_abs_p),
      ]:
      if hdfs.default_is_local():
        self.assertEqual(hdfs.path.abspath(p, **kw), local_abs_p)
      else:
        self.assertEqual(hdfs.path.abspath(p, **kw), r)


class TestBasename(unittest.TestCase):

  def good(self):
    self.assertEqual(hdfs.path.basename("hdfs://localhost:9000/foo/bar"), "bar")


class TestExists(unittest.TestCase):

  def good(self):
    path = uuid.uuid4().hex
    hdfs.dump("foo\n", path)
    self.assertTrue(hdfs.path.exists(path))
    hdfs.rmr(path)
    self.assertFalse(hdfs.path.exists(path))


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestSplit('good'))
  suite.addTest(TestSplit('good_with_user'))
  suite.addTest(TestSplit('bad'))
  suite.addTest(TestJoin('good'))
  suite.addTest(TestAbspath('good'))
  suite.addTest(TestBasename('good'))
  suite.addTest(TestExists('good'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
