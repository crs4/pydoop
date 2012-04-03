# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest, uuid
import pydoop.hdfs as hdfs
import pydoop.hdfs.config as pconf


class TestSplit(unittest.TestCase):

  def good(self):
    for p, r in [
      ('hdfs://localhost:9000/', ('localhost', 9000, '/')),
      ('hdfs://localhost:9000/a/b', ('localhost', 9000, '/a/b')),
      ('hdfs://localhost/a/b', ('localhost', pconf.DEFAULT_PORT, '/a/b')),
      ('hdfs:///a/b', ('default', 0, '/a/b')),
      ('file:///a/b', ('', 0, '/a/b')),
      ('file:/a/b', ('', 0, '/a/b')),
      ('file:///a', ('', 0, '/a')),
      ('file:/a', ('', 0, '/a')),
      ('file://localhost:9000/a/b', ('', 0, '/localhost:9000/a/b')),
      ('//localhost:9000/a/b', ('localhost', 9000, '/a/b')),
      ('/a/b', ('default', 0, '/a/b')),
      ('a/b', ('default', 0, '/user/%s/a/b' % pconf.DEFAULT_USER)),
      ]:
      self.assertEqual(hdfs.path.split(p), r)

  def good_with_user(self):
    for p, u, r in [
      ('a/b', None, ('default', 0, '/user/%s/a/b' % pconf.DEFAULT_USER)),
      ('a/b', pconf.DEFAULT_USER,
       ('default', 0, '/user/%s/a/b' % pconf.DEFAULT_USER)),
      ('a/b', 'foo', ('default', 0, '/user/foo/a/b')),
      ]:
      self.assertEqual(hdfs.path.split(p, u), r)

  def bad(self):
    for p in [
    'ftp://localhost:9000/',          # bad scheme
    'hdfs://localhost:spam/',         # port is not an int
    'hdfs://localhost:9000',          # path part is empty
    'hdfs://localhost:9000/a:b',      # colon outside netloc
    '/localhost:9000/a/b',            # colon outside netloc
    ]:
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
    for kw, r in [
      ({"user": None, "local": False},
       '%s/user/%s/%s' % (self.root, pconf.DEFAULT_USER, p)),
      ({"user": "pydoop", "local": False},
       '%s/user/pydoop/%s' % (self.root, p)),
      ({"user": None, "local": True},
       'file:%s' % (os.path.abspath(p))),
      ]:
      self.assertEqual(hdfs.path.abspath(p, **kw), r)
    p = 'file:%s' % (os.path.abspath(p))
    for kw, r in [
      ({"user": None, "local": False}, p),
      ({"user": None, "local": True}, 'file:%s' % (os.path.abspath(p))),
      ]:
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
