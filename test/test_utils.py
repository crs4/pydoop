# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest
import pydoop.utils as pu
import pydoop._pipes as pp


class split_hdfs_path_tc(unittest.TestCase):

  def good(self):
    for p, r in [
      ('hdfs://localhost:9000/', ('localhost', 9000, '/')),
      ('hdfs://localhost:9000/a/b', ('localhost', 9000, '/a/b')),
      ('hdfs://localhost/a/b', ('localhost', pu.DEFAULT_PORT, '/a/b')),
      ('hdfs:///a/b', ('default', 0, '/a/b')),
      ('file:///a/b', ('', 0, '/a/b')),
      ('file:/a/b', ('', 0, '/a/b')),
      ('file:///a', ('', 0, '/a')),
      ('file:/a', ('', 0, '/a')),
      ('file://localhost:9000/a/b', ('', 0, '/localhost:9000/a/b')),
      ('//localhost:9000/a/b', ('localhost', 9000, '/a/b')),
      ('/a/b', ('default', 0, '/a/b')),
      ('a/b', ('default', 0, '/user/%s/a/b' % pu.DEFAULT_USER)),
      ]:
      self.assertEqual(pu.split_hdfs_path(p), r)

  def good_with_user(self):
    for p, u, r in [
      ('a/b', None, ('default', 0, '/user/%s/a/b' % pu.DEFAULT_USER)),
      ('a/b', pu.DEFAULT_USER,
       ('default', 0, '/user/%s/a/b' % pu.DEFAULT_USER)),
      ('a/b', 'foo', ('default', 0, '/user/foo/a/b')),
      ]:
      self.assertEqual(pu.split_hdfs_path(p, u), r)

  def bad(self):
    for p, r in [
    ('ftp://localhost:9000/', ()),          # bad scheme
    ('hdfs://localhost:spam/', ()),         # port is not an int
    ('hdfs://localhost:9000', ()),          # path part is empty
    ('hdfs://localhost:9000/a:b', ()),      # colon outside netloc
    ('/localhost:9000/a/b', ()),            # colon outside netloc
    ]:
      self.assertRaises(ValueError, pu.split_hdfs_path, p)


configure_examples = {
  'a' : ['str', 'this is a string'],
  'b' : ['int', '22'],
  'b1' : ['int', '23'],
  'c' : ['float', '0.22'],
  'c1' : ['float', '0.0202'],
  'c2' : ['float', '.22'],
  'c3' : ['float', '1.0e-22'],
  'd' : ['bool' , 'false'],
  'd1' : ['bool' , 'true'],
  }


class utils_tc(unittest.TestCase):

  def jc_configure_plain(self):
    w = configure_examples
    d = {}
    for k in w.keys():
      d[k] = w[k][1]
    jc = pp.get_JobConf_object(d)
    class O(object):
      pass
    o = O()
    for k in w.keys():
      self.assertTrue(jc.hasKey(k))
      if w[k][0] == 'str':
        pu.jc_configure(o, jc, k, k)
        self.assertEqual(getattr(o,k), w[k][1])
      elif w[k][0] == 'int':
        pu.jc_configure_int(o, jc, k, k)
        self.assertEqual(getattr(o, k), int(w[k][1]))
      elif w[k][0] == 'bool':
        pu.jc_configure_bool(o, jc, k, k)
        self.assertEqual(getattr(o, k), w[k][1] == 'true')
      elif w[k][0] == 'float':
        pu.jc_configure_float(o, jc, k, k)
        self.assertAlmostEqual(getattr(o, k), float(w[k][1]))

  def jc_configure_default(self):
    w = configure_examples
    d = {}
    for k in w.keys():
      d[k] = w[k][1]
    jc = pp.get_JobConf_object(d)
    class O(object):
      pass
    o = O()
    for k in w.keys():
      nk = 'not-here-%s' % k
      self.assertFalse(jc.hasKey(nk))
      if w[k][0] == 'str':
        pu.jc_configure(o, jc, nk, k, w[k][1])
        self.assertEqual(getattr(o,k), w[k][1])
      elif w[k][0] == 'int':
        pu.jc_configure_int(o, jc, nk, k, int(w[k][1]))
        self.assertEqual(getattr(o, k), int(w[k][1]))
      elif w[k][0] == 'bool':
        pu.jc_configure_bool(o, jc, nk, k, w[k][1]=='true')
        self.assertEqual(getattr(o, k), w[k][1] == 'true')

  def jc_configure_no_default(self):
    w = configure_examples
    d = {}
    for k in w.keys():
      d[k] = w[k][1]
    jc = pp.get_JobConf_object(d)
    class O(object):
      pass
    o = O()
    for k in w.keys():
      nk = 'not-here-%s' % k
      self.assertFalse(jc.hasKey(nk))
      self.assertRaises(UserWarning, pu.jc_configure, o, jc, nk, k)

  def hadoop_serialization(self):
    for k in range(-256,256, 4):
      b = pp.serialize_int(k)
      (o, v) = pp.deserialize_int(b, 0)
      self.assertEqual(k, v)
    for k in range(-32000,32000, 100):
      b = pp.serialize_int(k)
      (o, v) = pp.deserialize_int(b, 0)
      self.assertEqual(k, v)
    for k in [-0.233, 232.11, 1e-9, 1e+12]:
      b = pp.serialize_float(k)
      (o, v) = pp.deserialize_float(b, 0)
      self.assertAlmostEqual((k-v)/(k+v), 0, 5)
    for k in ['fpp', 'eee', 'ddd']:
      b = pp.serialize_string(k)
      (o, v) = pp.deserialize_string(b, 0)
      self.assertEqual(k, v)
    things = [1233, 0.333, 'hello_there', '22', -0.5]
    b = ''
    for t in things:
      b += my_serialize(t)
    o = 0
    for t in things:
      equal_test = self.assertEqual
      if type(t) == int:
        (o, v) = pp.deserialize_int(b, o)
      elif type(t) == float:
        (o, v) = pp.deserialize_float(b, o)
        equal_test = self.assertAlmostEqual
      elif type(t) == str:
        (o, v) = pp.deserialize_string(b, o)
      equal_test(v, t)


def my_serialize(t):
  tt = type(t)
  if tt == int:
    return pp.serialize_int(t)
  if tt == float:
    return pp.serialize_float(t)
  if tt == str:
    return pp.serialize_string(t)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(utils_tc('jc_configure_plain'))
  suite.addTest(utils_tc('jc_configure_default'))
  suite.addTest(utils_tc('jc_configure_no_default'))
  suite.addTest(utils_tc('hadoop_serialization'))
  suite.addTest(split_hdfs_path_tc('good'))
  suite.addTest(split_hdfs_path_tc('good_with_user'))
  suite.addTest(split_hdfs_path_tc('bad'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
