# BEGIN_COPYRIGHT
#
# Copyright 2009-2014 CRS4.
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

# DEV NOTE: this module is used by the setup script, so it MUST NOT
# rely on extension modules.

import os, subprocess as sp, glob, re, platform
import xml.dom.minidom as dom
from xml.parsers.expat import ExpatError

try:
  from config import DEFAULT_HADOOP_HOME
except ImportError:  # should only happen at compile time
  DEFAULT_HADOOP_HOME = None
SYSTEM = platform.system().lower()


class HadoopVersionError(Exception):

  def __init__(self, version_str):
    self.value = "unrecognized version string format: %r" % (version_str,)

  def __str__(self):
    return repr(self.value)


class HadoopXMLError(Exception):
  pass


def get_arch():
  if SYSTEM == 'darwin':
    return "", ""
  bits, _ = platform.architecture()
  if bits == "64bit":
    return "amd64", "64"
  return "i386", "32"


class HadoopVersion(object):
  """
  Stores Hadoop version information.

  Hadoop version strings are in the <MAIN>-<REST> format, where <MAIN>
  is in the typical dot-separated integers format, while <REST> is
  subject to a higher degree of variation.  Examples: '0.20.2',
  '0.20.203.0', '0.20.2-cdh3u4', '1.0.4-SNAPSHOT', '2.0.0-mr1-cdh4.1.0'.

  The constructor parses the version string and stores:

    * a ``main`` attribute for the <MAIN> part;
    * a ``cdh`` attribute for the cdh part, if present;
    * an ``ext`` attribute for other appendages, if present.

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
    if len(version) > 1:
      self.cdh, self.ext = self.__parse_rest(version[1])
    else:
      self.cdh = self.ext = ()
    self.__tuple = self.main + self.cdh + self.ext

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

  def is_cloudera(self):
    return bool(self.cdh)

  def has_deprecated_bs(self):
    return self.cdh[:2] >= (4, 3)

  def has_security(self):
    return self.cdh >= (3, 0, 0) or self.main >= (0, 20, 203)

  def has_variable_isplit_encoding(self):
    pf = PathFinder()
    return (self.tuple >= (2,0,0) and not self.is_cloudera()) or pf.is_yarn()

  @property
  def tuple(self):
    return self.__tuple

  def __lt__(self, other): return self.tuple.__lt__(other.tuple)
  def __le__(self, other): return self.tuple.__le__(other.tuple)
  def __eq__(self, other): return self.tuple.__eq__(other.tuple)
  def __ne__(self, other): return self.tuple.__ne__(other.tuple)
  def __gt__(self, other): return self.tuple.__gt__(other.tuple)
  def __ge__(self, other): return self.tuple.__ge__(other.tuple)

  def tag(self):
    parts = self.main
    if self.cdh:
      parts += ("cdh",) + self.cdh
    if self.ext:
      parts += self.ext
    return "_".join(map(str, parts))

  def __str__(self):
    return self.__str


def cdh_mr1_version(version):
  if not isinstance(version, HadoopVersion):
    version = HadoopVersion(version)
    rtype = str
  else:
    rtype = HadoopVersion
  if not version.is_cloudera():
    raise ValueError("%r is not a cdh version string" % (str(version),))
  mr1_version_str = str(version).replace("cdh", "mr1-cdh")
  return rtype(mr1_version_str)


def is_exe(fpath):
  return os.path.exists(fpath) and os.access(fpath, os.X_OK)


def is_readable(fpath):
  return os.path.exists(fpath) and os.access(fpath, os.R_OK)


def first_dir_in_glob(pattern):
  for path in sorted(glob.glob(pattern)):
    if os.path.isdir(path):
      return path


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


def hadoop_home_from_path():
  for path in os.getenv("PATH", "").split(os.pathsep):
    if is_exe(os.path.join(path, 'hadoop')):
      return os.path.dirname(path)


class PathFinder(object):
  """
  Encapsulates the logic to find paths and other info required by Pydoop.
  """
  CDH_HADOOP_EXEC = "/usr/bin/hadoop"
  CDH_HADOOP_HOME_PKG = "/usr/lib/hadoop"
  CDH_HADOOP_HOME_PARCEL = first_dir_in_glob(
    "/opt/cloudera/parcels/CDH-*/lib/hadoop"
    )

  def __init__(self):
    self.__hadoop_home = None
    self.__hadoop_exec = None
    self.__mapred_exec = None
    self.__hadoop_conf = None
    self.__hadoop_version = None  # str
    self.__hadoop_version_info = None  # HadoopVersion
    self.__is_cloudera = None
    self.__hadoop_params = None
    self.__hadoop_native = None
    self.__hadoop_classpath = None

  def reset(self):
    self.__init__()

  @staticmethod
  def __error(what, env_var):
    raise ValueError("%s not found, try setting %s" % (what, env_var))

  def hadoop_home(self, fallback=DEFAULT_HADOOP_HOME):
    if not self.__hadoop_home:
      self.__hadoop_home = (
        os.getenv("HADOOP_HOME") or
        fallback or
        first_dir_in_glob("/usr/lib/hadoop*") or
        first_dir_in_glob("/opt/hadoop*") or
        hadoop_home_from_path()
        )
    if not self.__hadoop_home:
      PathFinder.__error("hadoop home", "HADOOP_HOME")
    return self.__hadoop_home

  def mapred_exec(self, hadoop_home=None):
    if not self.__mapred_exec:
      if not (hadoop_home or os.getenv("HADOOP_HOME")):
        if is_exe(self.CDH_HADOOP_EXEC):
          self.__mapred_exec = self.CDH_HADOOP_EXEC
      else:
        mapred = os.path.join(hadoop_home or self.hadoop_home(), "bin", "mapred")
        if os.path.exists(mapred):
          self.__mapred_exec = mapred
        else:
          self.__mapred_exec = self.hadoop_exec(hadoop_home)
    return self.__mapred_exec

  def hadoop_exec(self, hadoop_home=None):
    if not self.__hadoop_exec:
      # allow overriding of package-installed hadoop exec
      if not (hadoop_home or os.getenv("HADOOP_HOME")):
        if is_exe(self.CDH_HADOOP_EXEC):
          self.__hadoop_exec = self.CDH_HADOOP_EXEC
      else:
        fn = os.path.join(hadoop_home or self.hadoop_home(), "bin", "hadoop")
        if is_exe(fn):
          self.__hadoop_exec = fn
    if not self.__hadoop_exec:
      PathFinder.__error("hadoop executable", "HADOOP_HOME or PATH")
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
            env.pop("HADOOP_HOME", None)
            p = sp.Popen(
              [hadoop, "version"], stdout=sp.PIPE, stderr=sp.PIPE, env=env,
              )
            out, err = p.communicate()
            if p.returncode:
              raise RuntimeError(err or out)
            self.__hadoop_version = out.splitlines()[0].split()[-1]
          except (OSError, IndexError):
            pass
    if not self.__hadoop_version:
      PathFinder.__error("hadoop version", "HADOOP_VERSION")
    return self.__hadoop_version

  def hadoop_version_info(self, hadoop_home=None):
    if not self.__hadoop_version_info:
      self.__hadoop_version_info = HadoopVersion(
        self.hadoop_version(hadoop_home)
        )
    return self.__hadoop_version_info

  def cloudera(self, version=None, hadoop_home=None):
    if not self.__is_cloudera:
      version_info = HadoopVersion(version or self.hadoop_version(hadoop_home))
      self.__is_cloudera = version_info.is_cloudera()
    return self.__is_cloudera

  def hadoop_conf(self, hadoop_home=None):
    if not self.__hadoop_conf:
      try:
        self.__hadoop_conf = os.environ["HADOOP_CONF_DIR"]
      except KeyError:
        if self.cloudera():
          candidate = '/etc/hadoop/conf'
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
        except (IOError, HadoopXMLError) as e:
          pass  # silently ignore, as in Hadoop
      self.__hadoop_params = params
    return self.__hadoop_params

  def hadoop_native(self, hadoop_home=None):
    if hadoop_home is None:
      hadoop_home = self.hadoop_home()
    if not self.__hadoop_native:
      v = self.hadoop_version_info(hadoop_home)
      if not v.cdh or v.cdh < (4, 0, 0):
        if v.main >= (2, 0, 0):
          self.__hadoop_native = os.path.join(
            hadoop_home, 'lib', 'native')
        else:
          self.__hadoop_native = os.path.join(
            hadoop_home, 'lib', 'native', 'Linux-%s-%s' % get_arch()
            )
      else:  # FIXME: this does not cover from-tarball installation
        if os.path.isdir(self.CDH_HADOOP_HOME_PKG):
          hadoop_home = self.CDH_HADOOP_HOME_PKG
        elif os.path.isdir(self.CDH_HADOOP_HOME_PARCEL or ""):
          hadoop_home = self.CDH_HADOOP_HOME_PARCEL
        else:
          raise RuntimeError("unsupported CDH deployment")
        self.__hadoop_native = os.path.join(hadoop_home, 'lib', 'native')
    return self.__hadoop_native

  def hadoop_classpath(self, hadoop_home=None):
    if hadoop_home is None:
      hadoop_home = self.hadoop_home()
    if not self.__hadoop_classpath:
      v = self.hadoop_version_info(hadoop_home)
      if not v.cdh or v.cdh < (4, 0, 0):
        if v.main >= (2, 0, 0):
          self.__hadoop_classpath = ':'.join(
            glob.glob(os.path.join(hadoop_home, 'hadoop*.jar')) +
              glob.glob(os.path.join(hadoop_home, 'share/hadoop/hdfs', '*.jar')) +
              glob.glob(os.path.join(hadoop_home, 'share/hadoop/common/', '*.jar')) +
              glob.glob(os.path.join(hadoop_home, 'share/hadoop/common/lib', '*.jar')) + 
              glob.glob(os.path.join(hadoop_home, 'share/hadoop/mapreduce', '*.jar')) 
            )
        else:
          self.__hadoop_classpath = ':'.join(
            glob.glob(os.path.join(hadoop_home, 'hadoop*.jar')) +
            glob.glob(os.path.join(hadoop_home, 'lib', '*.jar')) +
            glob.glob(os.path.join(hadoop_home, 'share/hadoop', '*.jar')) +
            glob.glob(os.path.join(hadoop_home, 'share/hadoop/lib', '*.jar'))
            )
      else:  # FIXME: this does not cover from-tarball installation
        mr1_home = "%s-0.20-mapreduce" % hadoop_home
        if self.is_yarn():
          self.__hadoop_classpath = ':'.join(
            glob.glob(os.path.join(hadoop_home, 'client', '*.jar')) + 
            glob.glob(os.path.join(mr1_home, 'hadoop*.jar')) +
            glob.glob(os.path.join(hadoop_home, "*.jar")) +
            glob.glob(os.path.join(hadoop_home, "lib/*.jar"))
          )
        else:
          if os.path.isdir(self.CDH_HADOOP_HOME_PKG):
            hadoop_home = self.CDH_HADOOP_HOME_PKG
          elif os.path.isdir(self.CDH_HADOOP_HOME_PARCEL or ""):
            hadoop_home = self.CDH_HADOOP_HOME_PARCEL
          else:
            raise RuntimeError("unsupported CDH deployment")
          
          self.__hadoop_classpath = ':'.join(
            glob.glob(os.path.join(hadoop_home, 'client', '*.jar')) +
            glob.glob(os.path.join(hadoop_home, 'hadoop-annotations*.jar')) +
            glob.glob(os.path.join(mr1_home, 'hadoop*.jar'))
            )
        
      self.__hadoop_classpath += ":" + self.hadoop_native()
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
    return self.hadoop_params(hadoop_conf, hadoop_home).get('mapreduce.framework.name', '').lower() == 'yarn'
