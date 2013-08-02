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

import unittest, tempfile, os, stat, shutil, logging
import subprocess as sp
from xml.dom.minidom import getDOMImplementation
DOM_IMPL = getDOMImplementation()

import pydoop
from pydoop.jc import jc_wrapper
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
    self.hadoop_conf = os.path.join(self.hadoop_home, "conf")
    os.mkdir(self.hadoop_conf)
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
    for vs, main, cdh, ext in [
      ("0.20.2", (0, 20, 2), (), ()),
      ("0.20.203.0", (0, 20, 203, 0), (), ()),
      ("0.20.2-cdh3u4", (0, 20, 2), (3, 2, 4), ()),
      ("1.0.4-SNAPSHOT", (1, 0, 4), (), ("SNAPSHOT",)),
      ("2.0.0-mr1-cdh4.1.0", (2, 0, 0), (4, 1, 0), ("mr1",)),
      ("0.20.2+320", (0, 20, 2), (3, 0, 320), ()),
      ]:
      v = hu.HadoopVersion(vs)
      for name, attr in ("main", main), ("cdh", cdh), ("ext", ext):
        self.assertEqual(getattr(v, name), attr)
      self.assertEqual(v.is_cloudera(), len(v.cdh) > 0)
      self.assertEqual(v.tuple, main+cdh+ext)
    for s in "bla", '0.20.str', '0.20.2+str':
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
      self.assertEqual(vinfo.tuple, vt)
    # hadoop version from executable
    self.pf.reset()
    del os.environ["HADOOP_VERSION"]
    vinfo = self.pf.hadoop_version_info(self.hadoop_home)
    self.assertEqual(vinfo.main, self.hadoop_version_tuple)
    self.assertEqual(vinfo.tuple, self.hadoop_version_tuple)

  def test_get_hadoop_params(self):
    self.__check_params()
    self.__check_params('', {})
    self.__check_params('<?xml version="1.0"?>', {})
    doc = DOM_IMPL.createDocument(None, "configuration", None)
    self.__check_params(doc.toxml(), {})
    root = doc.documentElement
    prop = root.appendChild(doc.createElement("property"))
    self.__check_params(doc.toxml(), {})
    for s in "name", "value":
      n = prop.appendChild(doc.createElement(s))
      n.appendChild(doc.createTextNode(s.upper()))
    self.__check_params(doc.toxml(), {"NAME": "VALUE"})

  def __check_params(self, xml_content=None, expected=None):
    if expected is None:
      expected = {}
    xml_fn = os.path.join(self.hadoop_conf, "core-site.xml")
    if os.path.exists(xml_fn):
      os.remove(xml_fn)
    if xml_content is not None:
      with open(xml_fn, "w") as fo:
        fo.write(xml_content)
    params = self.pf.hadoop_params(hadoop_conf=self.hadoop_conf)
    self.assertEqual(params, expected)


class TestJcWrapper(unittest.TestCase):

  def setUp(self):
    self.data = {
      'int': '2',
      'float': '3.0',
      'bool_t': 't',
      'bool_T': 'T',
      'bool_true': 'true',
      'bool_True': 'TRUE',
      'bool_TRUE': 'TRUE',
      'bool_1': '1',
      'bool_f': 'f',
      'bool_F': 'F',
      'bool_false': 'false',
      'bool_False': 'False',
      'bool_FALSE': 'FALSE',
      'bool_0': '0',
      'str': 'str',
    }
    self.jc = pp.get_JobConf_object(self.data)
    self.wrapper = jc_wrapper(self.jc)

  def test_has_key(self):
    self.assertTrue(self.wrapper.has_key('int'))
    self.assertFalse(self.wrapper.has_key('no_key'))

  def test_simple_fetch(self):
    self.assertEqual('str', self.wrapper['str'])

  def test_fetch_missing(self):
    self.assertRaises(KeyError, lambda x: self.wrapper[x], 'no_key')

  def test_simple_get(self):
    self.assertEqual("2", self.wrapper.get('int'))
    self.assertTrue(self.wrapper.get('no_key') is None)
    # ensure caching doesn't cause problems
    self.assertEqual("2", self.wrapper.get('int'))
    self.assertTrue(self.wrapper.get('no_key') is None)

  def test_simple_get_default(self):
    self.assertEqual("default", self.wrapper.get('no_key', "default"))

  def test_get_boolean(self):
    for k in (
      'bool_t', 'bool_T', 'bool_true', 'bool_True', 'bool_TRUE', 'bool_1'
      ):
      self.assertEqual(True, self.wrapper.get_boolean(k))
    for k in (
      'bool_f', 'bool_F', 'bool_false', 'bool_False', 'bool_FALSE', 'bool_0'
      ):
      self.assertEqual(False, self.wrapper.get_boolean(k))
    # repeat to test cache
    for k in (
      'bool_f', 'bool_F', 'bool_false', 'bool_False', 'bool_FALSE', 'bool_0'
      ):
      self.assertEqual(False, self.wrapper.get_boolean(k))

  def test_get_bad_boolean(self):
    self.assertRaises(ValueError, self.wrapper.get_boolean, 'float')

  def test_get_missing_boolean(self):
    self.assertEqual(True, self.wrapper.get_boolean('no_key', True))

  def test_get_int(self):
    self.assertEqual(2, self.wrapper.get_int('int'))
    # cache test
    self.assertEqual(2, self.wrapper.get_int('int'))

  def test_get_bad_int(self):
    self.assertRaises(ValueError, self.wrapper.get_int, 'bool_f')

  def test_get_float_as_int(self):
    self.assertEqual(3, self.wrapper.get_int('float'))

  def test_get_missing_int(self):
    self.assertEqual(42, self.wrapper.get_int('no_key', 42))

  def test_get_float(self):
    self.assertEqual(3.0, self.wrapper.get_float('float'))

  def test_get_bad_float(self):
    self.assertRaises(ValueError, self.wrapper.get_float, 'bool_f')

  def test_get_int_as_float(self):
    self.assertEqual(2.0, self.wrapper.get_float('int'))

  def test_get_missing_float(self):
    self.assertEqual(42.0, self.wrapper.get_float('no_key', 42.0))


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestUtils('test_jc_configure_plain'))
  suite.addTest(TestUtils('test_jc_configure_default'))
  suite.addTest(TestUtils('test_jc_configure_no_default'))
  suite.addTest(TestUtils('test_hadoop_serialization'))
  suite.addTest(TestHadoopUtils('test_HadoopVersion'))
  suite.addTest(TestHadoopUtils('test_get_hadoop_exec'))
  suite.addTest(TestHadoopUtils('test_get_hadoop_version'))
  suite.addTest(TestHadoopUtils('test_get_hadoop_params'))
  suite.addTest(TestJcWrapper('test_has_key'))
  suite.addTest(TestJcWrapper('test_simple_fetch'))
  suite.addTest(TestJcWrapper('test_fetch_missing'))
  suite.addTest(TestJcWrapper('test_simple_get'))
  suite.addTest(TestJcWrapper('test_simple_get_default'))
  suite.addTest(TestJcWrapper('test_get_boolean'))
  suite.addTest(TestJcWrapper('test_get_bad_boolean'))
  suite.addTest(TestJcWrapper('test_get_missing_boolean'))
  suite.addTest(TestJcWrapper('test_get_int'))
  suite.addTest(TestJcWrapper('test_get_bad_int'))
  suite.addTest(TestJcWrapper('test_get_float_as_int'))
  suite.addTest(TestJcWrapper('test_get_missing_int'))
  suite.addTest(TestJcWrapper('test_get_float'))
  suite.addTest(TestJcWrapper('test_get_bad_float'))
  suite.addTest(TestJcWrapper('test_get_int_as_float'))
  suite.addTest(TestJcWrapper('test_get_missing_float'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
