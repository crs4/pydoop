# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest
import pydoop.hdfs.path as hpath
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
      self.assertEqual(hpath.split(p), r)

  def good_with_user(self):
    for p, u, r in [
      ('a/b', None, ('default', 0, '/user/%s/a/b' % pconf.DEFAULT_USER)),
      ('a/b', pconf.DEFAULT_USER,
       ('default', 0, '/user/%s/a/b' % pconf.DEFAULT_USER)),
      ('a/b', 'foo', ('default', 0, '/user/foo/a/b')),
      ]:
      self.assertEqual(hpath.split(p, u), r)

  def bad(self):
    for p in [
    'ftp://localhost:9000/',          # bad scheme
    'hdfs://localhost:spam/',         # port is not an int
    'hdfs://localhost:9000',          # path part is empty
    'hdfs://localhost:9000/a:b',      # colon outside netloc
    '/localhost:9000/a/b',            # colon outside netloc
    ]:
      self.assertRaises(ValueError, hpath.split, p)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestSplit('good'))
  suite.addTest(TestSplit('good_with_user'))
  suite.addTest(TestSplit('bad'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
