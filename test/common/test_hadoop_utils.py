# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import unittest
import tempfile
import os
import stat
import shutil
import subprocess as sp

from xml.dom.minidom import getDOMImplementation

DOM_IMPL = getDOMImplementation()

import pydoop.hadoop_utils as hu


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
        for vs, main, dist, dver, dext in [
            ("0.20.2", (0, 20, 2), 'apache', (), ()),
            ("0.20.203.0", (0, 20, 203, 0), 'apache', (), ()),
            ("0.20.2-cdh3u4", (0, 20, 2), 'cdh', (3, 2, 4), ()),
            ("1.0.4-SNAPSHOT", (1, 0, 4), 'apache', (), ("SNAPSHOT",)),
            ("2.0.0-mr1-cdh4.1.0", (2, 0, 0), 'cdh', (4, 1, 0), ("mr1",)),
            ("0.20.2+320", (0, 20, 2), 'apache', (3, 0, 320), ()),
            ("2.6.0.2.2.0.0-2041", (2, 6, 0), 'hdp', (2, 2, 0, 0), ('2041',)),
        ]:
            v = hu.HadoopVersion(vs)
            for name, attr in ("main", main), ('distribution', dist), \
                              ("dist_version", dver), ("dist_ext", dext):
                self.assertEqual(getattr(v, name), attr)
            self.assertEqual(v.is_cloudera(), dist == 'cdh')
            self.assertEqual(v.is_apache(), dist == 'apache')
            self.assertEqual(v.is_hortonworks(), dist == 'hdp')
            self.assertEqual(v.tuple, main + dver + dext)
            # minimal check -- tag is currently unused
            self.assertTrue(dist in v.tag())
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
        self.assertTrue(
            out.splitlines()[0].strip().lower().startswith(b"hadoop")
        )

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


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestHadoopUtils('test_HadoopVersion'))
    suite.addTest(TestHadoopUtils('test_get_hadoop_exec'))
    suite.addTest(TestHadoopUtils('test_get_hadoop_version'))
    suite.addTest(TestHadoopUtils('test_get_hadoop_params'))
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run((suite()))
