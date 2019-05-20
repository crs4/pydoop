# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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

# DEV NOTE: this module is used by the setup script, so it MUST be
# importable even if Pydoop has not been installed (yet).

"""
Tools for retrieving Hadoop-related information.
"""

import os
import glob
import re
import platform
import subprocess
import xml.dom.minidom as dom
from xml.parsers.expat import ExpatError


class HadoopXMLError(Exception):
    pass


def extract_text(node):
    return "".join(
        c.data.strip() for c in node.childNodes if c.nodeType == c.TEXT_NODE
    )


def parse_hadoop_conf_file(fn):
    items = []
    try:
        doc = dom.parse(fn)
    except ExpatError as e:
        raise HadoopXMLError("not a valid XML file (%s)" % e)
    conf = doc.documentElement
    if conf.nodeName != "configuration":
        raise HadoopXMLError("not a valid Hadoop configuration file")
    props = [n for n in conf.childNodes if n.nodeName == "property"]
    nv = {}
    for p in props:
        for n in p.childNodes:
            if n.childNodes:
                nv[n.nodeName] = extract_text(n)
        try:
            items.append((nv["name"], nv["value"]))
        except KeyError:
            pass
    return dict(items)


class PathFinder(object):
    """
    Encapsulates the logic to find paths and other info required by Pydoop.
    """
    def __init__(self):
        self.__hadoop_home = None
        self.__hadoop_conf = None
        self.__hadoop_params = None
        self.__hadoop_classpath = None
        self.__is_local = None

    def reset(self):
        self.__init__()

    # note that this can be None even after trying detection
    def hadoop_home(self):
        if not self.__hadoop_home:
            hh = os.getenv("HADOOP_HOME", os.getenv("HADOOP_PREFIX"))
            if not hh:
                exe = subprocess.check_output(
                    "command -v hadoop", shell=True, universal_newlines=True
                ).strip()
                candidate, child = os.path.split(os.path.dirname(exe))
                if child == "bin" and os.path.isdir(candidate):
                    hh = os.environ["HADOOP_HOME"] = candidate
            self.__hadoop_home = hh
        return self.__hadoop_home

    def hadoop_conf(self):
        if not self.__hadoop_conf:
            error = "Hadoop config not found, try setting HADOOP_CONF_DIR"
            try:
                self.__hadoop_conf = os.environ["HADOOP_CONF_DIR"]
            except KeyError:
                hh = self.hadoop_home()
                if not hh:
                    raise RuntimeError(error)
                candidate = os.path.join(hh, 'etc', 'hadoop')
                if not os.path.isdir(candidate):
                    raise RuntimeError(error)
                self.__hadoop_conf = os.environ["HADOOP_CONF_DIR"] = candidate
        return self.__hadoop_conf

    def hadoop_params(self):
        if not self.__hadoop_params:
            params = {}
            hadoop_conf = self.hadoop_conf()
            for n in "hadoop", "core", "hdfs", "mapred":
                fn = os.path.join(hadoop_conf, "%s-site.xml" % n)
                try:
                    params.update(parse_hadoop_conf_file(fn))
                except (IOError, HadoopXMLError):
                    pass  # silently ignore, as in Hadoop
            self.__hadoop_params = params
        return self.__hadoop_params

    def hadoop_classpath(self):
        if not self.__hadoop_classpath:
            cp = subprocess.check_output(
                "hadoop classpath --glob", shell=True, universal_newlines=True
            ).strip()
            # older hadoop versions ignore --glob
            if 'hadoop-common' not in cp:
                cp = ':'.join(':'.join(glob.iglob(_)) for _ in cp.split(':'))
            self.__hadoop_classpath = cp
        return self.__hadoop_classpath

    def __get_is_local(self):
        conf = self.hadoop_params()
        keys = ('mapreduce.framework.name',
                'mapreduce.jobtracker.address',
                'mapred.job.tracker')
        for k in keys:
            if conf.get(k, 'local').lower() != 'local':
                return False
        return True

    def is_local(self):
        """\
        Is Hadoop configured to run in local mode?

        By default, it is. [pseudo-]distributed mode must be
        explicitly configured.
        """
        if self.__is_local is None:
            self.__is_local = self.__get_is_local()
        return self.__is_local
