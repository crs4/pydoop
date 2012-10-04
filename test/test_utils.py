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

import unittest, tempfile, os, stat, shutil, logging
import subprocess as sp

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
    self.orig_env = os.environ.copy()
    self.pf = hu.PathFinder()

  def tearDown(self):
    os.environ.clear()
    os.environ.update(self.orig_env)
    shutil.rmtree(self.hadoop_home)

  def test_HadoopVersion(self):
    for vs, vmain, vext, is_cloudera in [
      ("0.20.2", (0, 20, 2), (), False),
      ("0.20.203.0", (0, 20, 203, 0), (), False),
      ("0.20.2-cdh3u4", (0, 20, 2), ("cdh3u4",), True),
      ("1.0.4-SNAPSHOT", (1, 0, 4), ("SNAPSHOT",), False),
      ]:
      v = hu.HadoopVersion(vs)
      self.assertEqual(v.main, vmain)
      self.assertEqual(v.ext, vext)
      self.assertEqual(v.is_cloudera(), is_cloudera)
      self.assertEqual(v.tuple(), vmain+vext)
    for s in "bla", '0.20.str':
      self.assertRaises(hu.HadoopVersionError, hu.HadoopVersion, s)

  def test_get_hadoop_exec(self):
    # hadoop home as argument
    self.assertEqual(
      self.pf.hadoop_exec(hadoop_home=self.hadoop_home), self.hadoop_exe
      )
    # hadoop home from environment
    os.environ["HADOOP_HOME"] = self.hadoop_home
    self.assertEqual(self.pf.hadoop_exec(), self.hadoop_exe)
    # no hadoop home in environment
    del os.environ["HADOOP_HOME"]
    os.environ["PATH"] = self.bindir
    hadoop_exec = self.pf.hadoop_exec()
    cmd = sp.Popen([hadoop_exec, "version"], env=self.orig_env,
                   stdout=sp.PIPE, stderr=sp.PIPE)
    out, _ = cmd.communicate()
    self.assertTrue(out.splitlines()[0].strip().lower().startswith("hadoop"))

  def test_get_hadoop_version(self):
    # hadoop version from environment
    vs = "0.21.0"
    vt = (0, 21, 0)
    os.environ["HADOOP_VERSION"] = vs
    for hadoop_home in None, self.hadoop_home:
      self.assertEqual(self.pf.hadoop_version(hadoop_home), vs)
      vinfo = self.pf.hadoop_version_info(hadoop_home)
      self.assertEqual(vinfo.main, vt)
      self.assertEqual(vinfo.tuple(), vt)
    # hadoop version from executable
    self.pf.reset()
    del os.environ["HADOOP_VERSION"]
    vinfo = self.pf.hadoop_version_info(self.hadoop_home)
    self.assertEqual(vinfo.main, self.hadoop_version_tuple)
    self.assertEqual(vinfo.tuple(), self.hadoop_version_tuple)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestUtils('test_jc_configure_plain'))
  suite.addTest(TestUtils('test_jc_configure_default'))
  suite.addTest(TestUtils('test_jc_configure_no_default'))
  suite.addTest(TestUtils('test_hadoop_serialization'))
  suite.addTest(TestHadoopUtils('test_HadoopVersion'))
  suite.addTest(TestHadoopUtils('test_get_hadoop_exec'))
  suite.addTest(TestHadoopUtils('test_get_hadoop_version'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
