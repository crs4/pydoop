# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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


CDH_HADOOP_HOME_PKG = '/usr/lib/hadoop'  # Cloudera bin packages
CDH_HADOOP_HOME_PARCEL = first_dir_in_glob(
    '/opt/cloudera/parcels/CDH-*/lib/hadoop'  # Cloudera Manager
)


class HadoopVersionError(Exception):

    def __init__(self, version_str):
        super(HadoopVersionError, self).__init__(
            'unrecognized version string format: %r' % (version_str,)
        )


class HadoopXMLError(Exception):
    pass


def get_arch():
    # if SYSTEM == 'darwin':
    #     return "", ""
    bits, _ = platform.architecture()
    if bits == "64bit":
        return "amd64", "64"
    return "i386", "32"


def _cdh_hadoop_home():
    if os.path.isdir(CDH_HADOOP_HOME_PKG):
        return CDH_HADOOP_HOME_PKG
    elif os.path.isdir(CDH_HADOOP_HOME_PARCEL or ''):
        return CDH_HADOOP_HOME_PARCEL
    raise RuntimeError("unsupported CDH deployment")


def _jars_from_dirs(dirs):
    jars = []
    for d in dirs:
        jars.extend(glob.glob(os.path.join(d, '*.jar')))
    return jars


def _hadoop_jars():
    pass


def _apache_hadoop_jars_v2(hadoop_home):
    jar_root = os.path.join(hadoop_home, 'share', 'hadoop')
    return _jars_from_dirs([os.path.join(jar_root, d) for d in (
        'hdfs',
        'common',
        os.path.join('common', 'lib'),
        'mapreduce',
        'yarn',  # hadoop >= 2.2.0
    )])


def _cdh_hadoop_jars_v2(hadoop_home):
    hadoop_hdfs = hadoop_home + "-hdfs"
    hadoop_yarn = hadoop_home + "-yarn"
    hadoop_mapred_v2 = hadoop_home + "-mapreduce"
    dirs = [hadoop_home, os.path.join(hadoop_home, "lib"),
            hadoop_hdfs, os.path.join(hadoop_hdfs, "lib"),
            hadoop_yarn, os.path.join(hadoop_yarn, "lib"),
            hadoop_mapred_v2, os.path.join(hadoop_mapred_v2, "lib")]
    jars = _jars_from_dirs(dirs)
    return jars


def _hdp_hadoop_jars_v2(hadoop_home):
    dirs = []
    for ext in ['', '-hdfs', '-yarn', '-mapreduce']:
        p = hadoop_home + ext
        dirs.extend([p, os.path.join(p, 'lib')])
    jars = _jars_from_dirs(dirs)
    return jars


class HadoopVersion(object):
    """
    Stores Hadoop version information.

    Hadoop version strings are in the <MAIN>-<REST> format, where <MAIN>
    is in the typical dot-separated integers format, while <REST> is
    subject to a higher degree of variation.  Examples: '0.20.2',
    '0.20.203.0', '0.20.2-cdh3u4', '1.0.4-SNAPSHOT', '2.0.0-mr1-cdh4.1.0',
    '2.6.0.2.2.0.0-2041'.

    Hadoop distribution detection is based on heuristics.
    Currently known hadoop distributions: Apache, Cloudera, Hortonworks.
    The attribute 'distribution' will contain, respectively, the string value
    'apache', 'cdh', 'hdp'.

    If the version string is not in the expected format, it raises
    ``HadoopVersionError``.  For consistency:

    * all attributes are stored as tuples
    * a minor version number is added to 'non-canonical' cdh version (if
      possible, this is done in a way that preserves ordering)

    CDH3 releases::

      [...]
      0.20.2+320
      0.20.2+737
      0.20.2-CDH3B4
      0.20.2-cdh3u0
      0.20.2-cdh3u1
      [...]

    CDH4 releases::

      0.23.0-cdh4b1
      0.23.1-cdh4.0.0b2
      2.0.0-cdh4.0.0
      2.0.0-cdh4.0.1
      2.0.0-cdh4.1.0
      [...]
    """
    def __init__(self, version_str):
        self.__str = version_str
        version = re.split(r"[-+]", self.__str, maxsplit=1)
        try:
            self.main = tuple(map(int, version[0].split(".")))
        except ValueError:
            raise HadoopVersionError(self.__str)

        if version_str.upper().find('CDH') > -1:
            self.distribution = 'cdh'
            self.dist_version, self.dist_ext = \
                self.__parse_rest(version[1])
        elif len(self.main) >= 7:
            self.distribution = 'hdp'
            self.dist_version = self.main[3:]
            self.main = self.main[:3]
            self.dist_ext = (version[1],)
        else:
            self.distribution = 'apache'
            self.dist_version, self.dist_ext = ((), ())
            if len(version) > 1:
                self.dist_version, self.dist_ext = \
                    self.__parse_rest(version[1])
        self.__tuple = (self.main + self.dist_version + self.dist_ext)

    def __parse_rest(self, rest_str):
        # older CDH3 releases
        if "+" in self.__str:
            try:
                rest = int(rest_str)
            except ValueError:
                raise HadoopVersionError(self.__str)
            else:
                return (3, 0, rest), ()
        # special cases
        elif rest_str == "CDH3B4":
            return (3, 1, 0), ("b4")
        elif rest_str == "cdh4b1":
            return (4, 0, 0), ("b1",)
        elif rest_str == "cdh4.0.0b2":
            return (4, 0, 0), ("b2",)
        # "canonical" version tags
        rest = rest_str.split("-", 1)
        rest.reverse()
        m = re.match(r"cdh(.+)", rest[0])
        if m is None:
            return (), tuple(rest)
        cdh_version_str = m.groups()[0]
        m = re.match(r"(\d+)u(\d+)", cdh_version_str)
        if m is None:
            cdh_version = cdh_version_str.split(".")
        else:
            cdh_version = m.groups()
        try:
            cdh_version = tuple(map(int, cdh_version))
        except ValueError:
            raise HadoopVersionError(self.__str)
        else:
            if len(cdh_version) == 2:
                assert cdh_version[0] == 3
                cdh_version = cdh_version[0], 2, cdh_version[1]
        return cdh_version, tuple(rest[1:])

    def is_apache(self):
        return self.distribution == 'apache'

    def is_cloudera(self):
        return self.distribution == 'cdh'

    def is_hortonworks(self):
        return self.distribution == 'hdp'

    def is_yarn(self):
        pf = PathFinder()
        return pf.is_yarn()

    def is_local(self):
        pf = PathFinder()
        return pf.is_local()

    def is_cdh_mrv2(self):
        return (self.distribution == 'cdh' and
                self.dist_version >= (4, 0, 0) and not self.dist_ext)

    def has_mrv2(self):
        return \
            self.main >= (2, 0, 0) and self.is_yarn() and \
            (not self.is_cloudera() or (
                self.is_cloudera() and self.dist_version >= (5, 0, 0)))

    def is_cdh_v5(self):
        return (self.distribution == 'cdh' and
                self.dist_version >= (5, 0, 0) and
                self.dist_version < (6, 0, 0))

    def has_deprecated_bs(self):
        return (self.distribution == 'cdh' and
                self.dist_version[:2] >= (4, 3))

    def has_security(self):
        return ((self.distribution == 'cdh' and
                 self.dist_version >= (3, 0, 0)) or
                self.main >= (0, 20, 203))

    def has_variable_isplit_encoding(self):
        pf = PathFinder()
        return ((self.tuple >= (2, 0, 0) and not self.is_cloudera()) or
                pf.is_yarn())

    @property
    def tuple(self):
        return self.__tuple

    def __lt__(self, other):
        return self.tuple.__lt__(other.tuple)

    def __le__(self, other):
        return self.tuple.__le__(other.tuple)

    def __eq__(self, other):
        return self.tuple.__eq__(other.tuple)

    def __ne__(self, other):
        return self.tuple.__ne__(other.tuple)

    def __gt__(self, other):
        return self.tuple.__gt__(other.tuple)

    def __ge__(self, other):
        return self.tuple.__ge__(other.tuple)

    def tag(self):
        parts = self.main + (self.distribution,)
        if self.dist_version:
            parts += self.dist_version
        if self.dist_ext:
            parts += self.dist_ext
        return "_".join(map(str, parts))

    def __str__(self):
        return self.__str


def is_exe(fpath):
    """
    Path references an executable file.
    """
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)


def is_readable(fpath):
    """
    Path references a readable file.
    """
    return os.path.isfile(fpath) and os.access(fpath, os.R_OK)


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
    CDH_HADOOP_EXEC = "/usr/bin/hadoop"  # CDH and rpm
    RPM_HADOOP_HOME = "/usr/share/hadoop"

    def __init__(self):
        self.__hadoop_home = None
        self.__hadoop_exec = None
        self.__mapred_exec = None
        self.__hadoop_conf = None
        self.__hadoop_version = None  # str
        self.__hadoop_version_info = None  # HadoopVersion
        self.__hadoop_params = None
        self.__hadoop_native = None
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

    def mapred_exec(self, hadoop_home=None):
        if not self.__mapred_exec:
            if not (hadoop_home or
                    os.getenv("HADOOP_PREFIX", os.getenv("HADOOP_HOME"))):
                if is_exe(self.CDH_HADOOP_EXEC):
                    self.__mapred_exec = self.CDH_HADOOP_EXEC
            else:
                mapred = os.path.join(
                    hadoop_home or self.hadoop_home(), "bin", "mapred"
                )
                if os.path.exists(mapred):
                    self.__mapred_exec = mapred
                else:
                    self.__mapred_exec = self.hadoop_exec(hadoop_home)
        return self.__mapred_exec

    def hadoop_exec(self, hadoop_home=None):
        if not self.__hadoop_exec:
            # allow overriding of package-installed hadoop exec
            if not (hadoop_home or
                    os.getenv("HADOOP_PREFIX", os.getenv("HADOOP_HOME"))):
                if is_exe(self.CDH_HADOOP_EXEC):
                    self.__hadoop_exec = self.CDH_HADOOP_EXEC
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

    def hadoop_version(self, hadoop_home=None):
        if not self.__hadoop_version:
            try:
                self.__hadoop_version = os.environ["HADOOP_VERSION"]
            except KeyError:
                try:
                    hadoop = self.hadoop_exec(hadoop_home)
                except ValueError:
                    pass
                else:
                    try:
                        env = os.environ.copy()
                        # why pop HADOOP_HOME?
                        env.pop("HADOOP_PREFIX", None)
                        env.pop("HADOOP_HOME", None)
                        p = sp.Popen([hadoop, "version"], stdout=sp.PIPE,
                                     stderr=sp.PIPE, env=env,
                                     universal_newlines=True)
                        out, err = p.communicate()
                        if p.returncode:
                            raise RuntimeError(err or out)
                        self.__hadoop_version = out.splitlines()[0].split()[-1]
                    except (OSError, IndexError):
                        pass
        if not self.__hadoop_version:
            PathFinder.__error("hadoop version", "HADOOP_VERSION")
        return self.__hadoop_version

    def is_cloudera(self, hadoop_home=None):
        return self.hadoop_version_info(hadoop_home).is_cloudera()

    def hadoop_version_info(self, hadoop_home=None):
        if not self.__hadoop_version_info:
            self.__hadoop_version_info = HadoopVersion(
                self.hadoop_version(hadoop_home)
            )
        return self.__hadoop_version_info

    def hadoop_conf(self, hadoop_home=None):
        if not self.__hadoop_conf:
            try:
                self.__hadoop_conf = os.environ["HADOOP_CONF_DIR"]
            except KeyError:
                if self.is_cloudera(hadoop_home):
                    candidate = '/etc/hadoop/conf'
                elif os.path.isdir(self.RPM_HADOOP_HOME):
                    candidate = '/etc/hadoop'
                else:
                    if not hadoop_home:
                        hadoop_home = self.hadoop_home()
                    v = self.hadoop_version_info(hadoop_home)
                    if v.main >= (2, 0, 0):
                        candidate = os.path.join(hadoop_home, 'etc', 'hadoop')
                    else:
                        candidate = os.path.join(hadoop_home, 'conf')
                if os.path.isdir(candidate):
                    self.__hadoop_conf = candidate
        if not self.__hadoop_conf:
            PathFinder.__error("hadoop conf dir", "HADOOP_CONF_DIR")
        os.environ["HADOOP_CONF_DIR"] = self.__hadoop_conf
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

    def hadoop_native(self, hadoop_home=None):
        if hadoop_home is None:
            hadoop_home = self.hadoop_home()
        if not self.__hadoop_native:
            if os.path.isdir(self.RPM_HADOOP_HOME):
                self.__hadoop_native = os.path.join(
                    '/usr', 'lib%s' % get_arch()[1]
                )
                return self.__hadoop_native
            v = self.hadoop_version_info(hadoop_home)
            if v.distribution == 'apache':
                if v.main >= (2, 0, 0):
                    self.__hadoop_native = os.path.join(
                        hadoop_home, 'lib', 'native'
                    )
                else:
                    self.__hadoop_native = os.path.join(
                        hadoop_home, 'lib', 'native',
                        'Linux-%s-%s' % get_arch()
                    )
            elif v.distribution == 'hdp':
                if v.main >= (2, 0, 0):
                    self.__hadoop_native = os.path.join(
                        hadoop_home, 'hadoop', 'lib', 'native'
                    )
                else:
                    raise RuntimeError('%s is not supported' % v)
            elif v.distribution == 'cdh':
                hadoop_home = _cdh_hadoop_home()
                self.__hadoop_native = os.path.join(
                    hadoop_home, 'lib', 'native'
                )
            else:
                raise RuntimeError('%s is not supported' % v)
        return self.__hadoop_native

    def hadoop_classpath(self, hadoop_home=None):
        if hadoop_home is None:
            hadoop_home = self.hadoop_home()
        if not self.__hadoop_classpath:
            v = self.hadoop_version_info(hadoop_home)
            if v.main < (2, 0, 0):
                raise RuntimeError('Hadoop v1 is not supported')
            if v.distribution == 'apache':
                jars = _apache_hadoop_jars_v2(hadoop_home)
            elif v.distribution == 'cdh':
                hadoop_home = _cdh_hadoop_home()
                jars = _cdh_hadoop_jars_v2(hadoop_home)
            elif v.distribution == 'hdp':
                jars = _hdp_hadoop_jars_v2(hadoop_home)
            jars.extend([self.hadoop_native(), self.hadoop_conf()])
            self.__hadoop_classpath = ':'.join(jars)
        return self.__hadoop_classpath

    def find(self):
        info = {}
        for a in (
            "hadoop_exec",
            "hadoop_version_info",
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

    def is_yarn(self, hadoop_conf=None, hadoop_home=None):
        return self.hadoop_params(hadoop_conf, hadoop_home).get(
            'mapreduce.framework.name', ''
        ).lower() == 'yarn'

    def is_local(self, hadoop_conf=None, hadoop_home=None):
        conf = self.hadoop_params(hadoop_conf, hadoop_home)
        framework_name = conf.get('mapreduce.framework.name', '').lower()
        if not framework_name:  # pre-yarn version
            framework_name = conf.get('mapred.job.tracker', '').lower()
        # We also interpret the empty string as 'local' since it's the
        # default value for both the properties above.
        return framework_name == 'local' or framework_name == ''
