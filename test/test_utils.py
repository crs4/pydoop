import unittest
import random

import sys

#----------------------------------------------------------------------------
from pydoop.utils import split_hdfs_path, jc_configure, jc_configure_int, jc_configure_bool
from pydoop.utils import jc_configure_float
from pydoop.utils import DEFAULT_HDFS_PORT

from pydoop_pipes import get_JobConf_object
#----------------------------------------------------------------------------

split_examples = [('canonical', 'hdfs://foobar.foo.com:1234/foofile/bar',
                   ('foobar.foo.com', 1234, '/foofile/bar')),
                  ('canonical', 'file:///foofile/bar',
                   ('', 0, '/foofile/bar')),
                  ('canonical', 'hdfs:///foofile/bar',
                   ('localhost', 0, '/foofile/bar')),
                  ('bad',       '/foobar.foo.com:1234/foofile/bar', ()),
                  ('bad',       'file://foobar.foo.com:1234/foofile/bar', ()),
                  ('canonical', 'hdfs://foobar.foo.com/foofile/bar',
                   ('foobar.foo.com', DEFAULT_HDFS_PORT, '/foofile/bar'))
                  ]

configure_examples = { 'a' : ['str', 'this is a string'],
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
  def split(self):
    for t, p, r in split_examples:
      print 'p=', p
      if t == 'bad':
        self.assertRaises(UserWarning, split_hdfs_path, p)
        try:
          split_hdfs_path(p)
        except Exception, e:
          m = 'pydoop_exception: split_hdfs_path: illegal hdfs path <%s>' % p
          self.assertEqual(e.args[0], m)
      else:
        self.assertEqual(split_hdfs_path(p), r)
  #--
  def jc_configure_plain(self):
    w = configure_examples
    d = {}
    for k in w.keys():
      d[k] = w[k][1]
    jc = get_JobConf_object(d)
    class O(object):
      pass
    o = O()
    for k in w.keys():
      self.assertTrue(jc.hasKey(k))
      if w[k][0] == 'str':
        jc_configure(o, jc, k, k)
        self.assertEqual(getattr(o,k), w[k][1])
      elif w[k][0] == 'int':
        jc_configure_int(o, jc, k, k)
        self.assertEqual(getattr(o, k), int(w[k][1]))
      elif w[k][0] == 'bool':
        jc_configure_bool(o, jc, k, k)
        self.assertEqual(getattr(o, k), w[k][1] == 'true')
      elif w[k][0] == 'float':
        jc_configure_float(o, jc, k, k)
        self.assertAlmostEqual(getattr(o, k), float(w[k][1]))
  #--
  def jc_configure_default(self):
    w = configure_examples
    d = {}
    for k in w.keys():
      d[k] = w[k][1]
    jc = get_JobConf_object(d)
    class O(object):
      pass
    o = O()
    for k in w.keys():
      nk = 'not-here-%s' % k
      self.assertFalse(jc.hasKey(nk))
      if w[k][0] == 'str':
        jc_configure(o, jc, nk, k, w[k][1])
        self.assertEqual(getattr(o,k), w[k][1])
      elif w[k][0] == 'int':
        jc_configure_int(o, jc, nk, k, int(w[k][1]))
        self.assertEqual(getattr(o, k), int(w[k][1]))
      elif w[k][0] == 'bool':
        jc_configure_bool(o, jc, nk, k, w[k][1]=='true')
        self.assertEqual(getattr(o, k), w[k][1] == 'true')
  #--
  def jc_configure_no_default(self):
    w = configure_examples
    d = {}
    for k in w.keys():
      d[k] = w[k][1]
    jc = get_JobConf_object(d)
    class O(object):
      pass
    o = O()
    for k in w.keys():
      nk = 'not-here-%s' % k
      self.assertFalse(jc.hasKey(nk))
      self.assertRaises(UserWarning, jc_configure, o, jc, nk, k)

#----------------------------------------------------------------------------
def suite():
  suite = unittest.TestSuite()
  #--
  suite.addTest(utils_tc('split'))
  suite.addTest(utils_tc('jc_configure_plain'))
  suite.addTest(utils_tc('jc_configure_default'))
  suite.addTest(utils_tc('jc_configure_no_default'))
  #--
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

