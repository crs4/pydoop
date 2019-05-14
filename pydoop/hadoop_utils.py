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
Tools for retrieving information on the available Hadoop
installation(s): version, classpath, etc.

Known issues: from-tarball CDH installation is not supported.
"""

import os
import glob
import re
import platform
import subprocess as sp
import xml.dom.minidom as dom
from xml.parsers.expat import ExpatError

SYSTEM = platform.system().lower()


def first_dir_in_glob(pattern):
    for path in sorted(glob.glob(pattern)):
        if os.path.isdir(path):
            return path


class HadoopXMLError(Exception):
    pass


def is_exe(fpath):
    """
    Path references an executable file.
    """
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)


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


def _hadoop_home_from_version_cmd():
    def get_hh_from_version_output(output):
        """
        the ``hadoop version`` command prints out some information.  The
        last line contains the absolute path to the ``hadoop-core``
        jar, which should be in the Hadoop home directory.
        """
        def find_hadoop_root(dpath):
            while not os.path.exists(os.path.join(dpath, 'README.txt')):
                dpath = os.path.dirname(dpath)
                if dpath == '/':
                    return None
            return dpath
        if not output:
            return None
        last_line = output.splitlines()[-1]
        m = re.match(r'This command was run using (.*\.jar)', last_line)
        if m:
            home = find_hadoop_root(os.path.dirname(m.group(1)))
            return home
        return None

    for path in os.getenv("PATH", "").split(os.pathsep):
        full_path = os.path.join(path, 'hadoop')
        if is_exe(full_path):
            hadoop_exec = full_path
            break
    else:
        hadoop_exec = None

    if hadoop_exec:
        try:
            output = sp.check_output([hadoop_exec, 'version'],
                                     universal_newlines=True)
            return get_hh_from_version_output(output)
        except sp.CalledProcessError:
            pass

    return None


class PathFinder(object):
    """
    Encapsulates the logic to find paths and other info required by Pydoop.
    """
    def __init__(self):
        self.__hadoop_home = None
        self.__hadoop_exec = None
        self.__hadoop_conf = None
        self.__hadoop_params = None
        self.__hadoop_classpath = None

    def reset(self):
        self.__init__()

    @staticmethod
    def __error(what, env_var):
        raise ValueError("%s not found, try setting %s" % (what, env_var))

    def hadoop_home(self):
        if not self.__hadoop_home:
            self.__hadoop_home = (
                os.getenv("HADOOP_HOME") or
                os.getenv("HADOOP_PREFIX") or
                _hadoop_home_from_version_cmd() or
                first_dir_in_glob("/usr/lib/hadoop*") or
                first_dir_in_glob("/usr/share/hadoop*") or
                first_dir_in_glob("/opt/hadoop*")
            )
        if not self.__hadoop_home:
            PathFinder.__error("hadoop home", "HADOOP_PREFIX or HADOOP_HOME")
        return self.__hadoop_home

    def hadoop_exec(self, hadoop_home=None):
        if not self.__hadoop_exec:
            if is_exe("/usr/bin/hadoop") and not hadoop_home:
                self.__hadoop_exec = "/usr/bin/hadoop"
            else:
                fn = os.path.join(
                    hadoop_home or self.hadoop_home(), "bin", "hadoop"
                )
                if is_exe(fn):
                    self.__hadoop_exec = fn
        if not self.__hadoop_exec:
            PathFinder.__error(
                "hadoop executable", "HADOOP_PREFIX or HADOOP_HOME or PATH"
            )
        return self.__hadoop_exec

    def hadoop_conf(self, hadoop_home=None):
        if not hadoop_home:
            hadoop_home = self.hadoop_home()
        if not self.__hadoop_conf:
            try:
                self.__hadoop_conf = os.environ["HADOOP_CONF_DIR"]
            except KeyError:
                candidate = os.path.join(hadoop_home, 'etc', 'hadoop')
                if not os.path.isdir(candidate):
                    PathFinder.__error("hadoop conf dir", "HADOOP_CONF_DIR")
                self.__hadoop_conf = candidate
                os.environ["HADOOP_CONF_DIR"] = candidate
        return self.__hadoop_conf

    def hadoop_params(self, hadoop_conf=None, hadoop_home=None):
        if not self.__hadoop_params:
            params = {}
            if not hadoop_conf:
                hadoop_conf = self.hadoop_conf(hadoop_home)
            for n in "hadoop", "core", "hdfs", "mapred":
                fn = os.path.join(hadoop_conf, "%s-site.xml" % n)
                try:
                    params.update(parse_hadoop_conf_file(fn))
                except (IOError, HadoopXMLError):
                    pass  # silently ignore, as in Hadoop
            self.__hadoop_params = params
        return self.__hadoop_params

    def hadoop_classpath(self, hadoop_home=None):
        if hadoop_home is None:
            hadoop_home = self.hadoop_home()
        if not self.__hadoop_classpath:
            hadoop = self.hadoop_exec(hadoop_home=hadoop_home)
            cmd = [hadoop, 'classpath', '--glob']
            cp = sp.check_output(cmd, universal_newlines=True).strip()
            # older hadoop versions ignore --glob
            if 'hadoop-common' not in cp:
                cp = ':'.join(':'.join(glob.iglob(_)) for _ in cp.split(':'))
            self.__hadoop_classpath = cp
        return self.__hadoop_classpath

    def find(self):
        info = {}
        for a in (
            "hadoop_exec",
            "hadoop_home",
            "hadoop_conf",
            "hadoop_params",
            "hadoop_classpath",
        ):
            try:
                info[a] = getattr(self, a)()
            except ValueError:
                info[a] = None
        return info

    def is_local(self, hadoop_conf=None, hadoop_home=None):
        """\
        Is Hadoop configured to run in local mode?

        By default, it is. [pseudo-]distributed mode must be
        explicitly configured.
        """
        conf = self.hadoop_params(hadoop_conf, hadoop_home)
        keys = ('mapreduce.framework.name',
                'mapreduce.jobtracker.address',
                'mapred.job.tracker')
        for k in keys:
            if conf.get(k, 'local').lower() != 'local':
                return False
        return True
