# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, tempfile, os, stat, shutil, logging

import pydoop
import pydoop.utils as pu
import pydoop.hadoop_utils as hu
pp = pydoop.import_version_specific_module('_pipes')


CONFIGURE_EXAMPLES = {
  # jobconf_key/attr_name: [type, str_value]
  'a' : ['str', 'this is a string'],
  'b' : ['int', '22'],
  'b1' : ['int', '23'],
  'c' : ['float', '0.22'],
  'c1' : ['float', '0.0202'],
  'c2' : ['float', '.22'],
  'c3' : ['float', '1.0e-22'],
  'd' : ['bool' , 'false'],
  'd1' : ['bool' , 'true'],
  'e' : ['log_level' , 'DEBUG'],
  }


class Obj(object):
  pass


def serialize(t):
  tt = type(t)
  if tt == int:
    return pp.serialize_int(t)
  if tt == float:
    return pp.serialize_float(t)
  if tt == str:
    return pp.serialize_string(t)


class TestUtils(unittest.TestCase):

  def test_jc_configure_plain(self):
    w = CONFIGURE_EXAMPLES
    d = {}
    for k in w.keys():
      d[k] = w[k][1]
    jc = pp.get_JobConf_object(d)
    o = Obj()
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
      elif w[k][0] == 'log_level':
        pu.jc_configure_log_level(o, jc, k, k)
        self.assertEqual(getattr(o, k), getattr(logging, w[k][1]))

  def test_jc_configure_default(self):
    w = CONFIGURE_EXAMPLES
    d = {}
    for k in w.keys():
      d[k] = w[k][1]
    jc = pp.get_JobConf_object(d)
    o = Obj()
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
      elif w[k][0] == 'log_level':
        pu.jc_configure_log_level(o, jc, nk, k, w[k][1])
        self.assertEqual(getattr(o, k), getattr(logging, w[k][1]))

  def test_jc_configure_no_default(self):
    w = CONFIGURE_EXAMPLES
    d = {}
    for k in w.keys():
      d[k] = w[k][1]
    jc = pp.get_JobConf_object(d)
    o = Obj()
    for k in w.keys():
      nk = 'not-here-%s' % k
      self.assertFalse(jc.hasKey(nk))
      self.assertRaises(UserWarning, pu.jc_configure, o, jc, nk, k)

  def test_hadoop_serialization(self):
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
      b += serialize(t)
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


class TestHadoopUtils(unittest.TestCase):

  def setUp(self):
    self.hadoop_version = "0.20.2"
    self.hadoop_version_tuple = (0, 20, 2)
    self.hadoop_home = tempfile.mkdtemp(prefix="pydoop_test_")
    self.bindir = os.path.join(self.hadoop_home, "bin")
    os.mkdir(self.bindir)
    self.hadoop_exe = os.path.join(self.bindir, "hadoop")
    with open(self.hadoop_exe, "w") as fo:
      fd = fo.fileno()
      os.fchmod(fd, os.fstat(fd).st_mode | stat.S_IXUSR)
      fo.write("#!/bin/bash\necho Hadoop %s\n" % self.hadoop_version)
    self.orig_hadoop_home = os.getenv("HADOOP_HOME")
    self.orig_path = os.getenv("PATH")
    self.orig_hadoop_version = os.getenv("HADOOP_VERSION")

  def tearDown(self):
    for var_name in "HADOOP_HOME", "PATH", "HADOOP_VERSION":
      orig_var = getattr(self, "orig_%s" % var_name.lower())
      if orig_var:
        os.environ[var_name] = orig_var
    shutil.rmtree(self.hadoop_home)

  def test_get_version_tuple(self):
    for vs, vt in [
      ("0.20.2", (0, 20, 2)),
      ("0.20.203.0", (0, 20, 203, "0")),
      ("0.20.3-cdh3", (0, 20, 3, "cdh3")),
      ("0.20.203.1-SNAPSHOT", (0, 20, 203, "1", "SNAPSHOT")),
      ]:
      self.assertEqual(hu.version_tuple(vs), vt)

  def test_get_hadoop_exec(self):
    # hadoop home as argument
    self.assertEqual(
      hu.get_hadoop_exec(hadoop_home=self.hadoop_home), self.hadoop_exe
      )
    # hadoop home from environment
    os.environ["HADOOP_HOME"] = self.hadoop_home
    self.assertEqual(hu.get_hadoop_exec(), self.hadoop_exe)
    # hadoop home from path
    del os.environ["HADOOP_HOME"]
    os.environ["PATH"] = self.bindir
    self.assertEqual(hu.get_hadoop_exec(), self.hadoop_exe)

  def test_get_hadoop_version(self):
    # hadoop version from environment
    vs = "0.21.0"
    vt = (0, 21, 0)
    os.environ["HADOOP_VERSION"] = vs
    for hadoop_home in None, self.hadoop_home:
      self.assertEqual(hu.get_hadoop_version(hadoop_home=hadoop_home), vt)
    # hadoop version from executable
    del os.environ["HADOOP_VERSION"]
    self.assertEqual(hu.get_hadoop_version(hadoop_home=self.hadoop_home),
                     self.hadoop_version_tuple)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestUtils('test_jc_configure_plain'))
  suite.addTest(TestUtils('test_jc_configure_default'))
  suite.addTest(TestUtils('test_jc_configure_no_default'))
  suite.addTest(TestUtils('test_hadoop_serialization'))
  suite.addTest(TestHadoopUtils('test_get_version_tuple'))
  suite.addTest(TestHadoopUtils('test_get_hadoop_exec'))
  suite.addTest(TestHadoopUtils('test_get_hadoop_version'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
