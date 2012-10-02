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

# DEV NOTE: this module is used by the setup script, so it MUST NOT
# rely on extension modules.

import os, subprocess as sp, glob
import xml.dom.minidom

try:
  from config import DEFAULT_HADOOP_HOME
except ImportError:  # should only happen at compile time
  DEFAULT_HADOOP_HOME = None


class HadoopVersionError(Exception):

  def __init__(self, version_str):
    self.value = "unrecognized version string format: %r" % (version_str,)

  def __str__(self):
    return repr(self.value)


class HadoopVersion(object):
  """
  Stores Hadoop version information.

  Hadoop version strings are in the <MAIN>-<EXT> format, where <MAIN>
  is in the typical dot-separated integers format, while <EXT> is
  subject to a higher degree of variation.  Examples: '0.20.2',
  '0.20.203.0', '0.20.2-cdh3u4', '1.0.4-SNAPSHOT'.

  The constructor parses the version string and stores a ``main`` and
  an ``ext`` attribute corresponding to the aforementioned sections;
  if the version string is not in the expected format, it raises
  ``HadoopVersionError``.
  """
  def __init__(self, version_str):
    version = version_str.split("-")
    try:
      self.main = tuple(map(int, version[0].split(".")))
    except ValueError:
      raise HadoopVersionError(version_str)
    try:
      self.ext = tuple(version[1].split("."))
    except IndexError:
      self.ext = tuple()

  def is_cloudera(self):
    return self.ext and self.ext[0].startswith("cdh")

  def tuple(self):
    return self.main + self.ext

  def __str__(self):
    s = ".".join(str(_) for _ in self.main)
    if self.ext:
      s = "%s-%s" % (s, ".".join(self.ext))
    return s


def is_exe(fpath):
  return os.path.exists(fpath) and os.access(fpath, os.X_OK)


def is_readable(fpath):
  return os.path.exists(fpath) and os.access(fpath, os.R_OK)


def version_tuple(version_str):
  return HadoopVersion(version_str).tuple()


def first_dir_in_glob(pattern):
  for path in sorted(glob.glob(pattern)):
    if os.path.isdir(path):
      return path


def parse_hadoop_conf_file(fn):
  items = []
  dom = xml.dom.minidom.parse(fn)
  conf = dom.getElementsByTagName("configuration")[0]
  props = conf.getElementsByTagName("property")
  for p in props:
    kv = []
    for tag_name in "name", "value":
      e = p.getElementsByTagName(tag_name)[0]
      kv.append("".join(
        n.data.strip() for n in e.childNodes if n.nodeType == n.TEXT_NODE
        ))
    items.append(tuple(kv))
  return dict(items)


def hadoop_home_from_path():
  for path in os.getenv("PATH", "").split(os.pathsep):
    if is_exe(os.path.join(path, 'hadoop')):
      return os.path.dirname(path)


class PathFinder(object):
  """
  Encapsulates the logic to find paths and other info required by Pydoop.
  """
  def __init__(self):
    self.__hadoop_home = None
    self.__hadoop_exec = None
    self.__hadoop_conf = None
    self.__hadoop_version = None  # str
    self.__hadoop_version_info = None  # tuple
    self.__is_cloudera = None
    self.__hadoop_params = None

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

  def hadoop_exec(self, hadoop_home=None):
    if not self.__hadoop_exec:
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
            out, err = sp.Popen(
              [hadoop, "version"], stdout=sp.PIPE, stderr=sp.PIPE
              ).communicate()
            if err and not out:
              raise RuntimeError(err)
            self.__hadoop_version = out.splitlines()[0].split()[-1]
          except (OSError, IndexError):
            pass
    if not self.__hadoop_version:
      PathFinder.__error("hadoop version", "HADOOP_VERSION")
    return self.__hadoop_version

  def hadoop_version_info(self, hadoop_home=None):
    if not self.__hadoop_version_info:
      self.__hadoop_version_info = version_tuple(
        self.hadoop_version(hadoop_home)
        )
    return self.__hadoop_version_info

  def cloudera(self, version=None, hadoop_home=None):
    if not self.__is_cloudera:
      version_info = version_tuple(version or self.hadoop_version(hadoop_home))
      for part in version_info[3:]:
        if part.startswith("cdh"):
          self.__is_cloudera = True
          break
    return self.__is_cloudera

  def hadoop_conf(self, hadoop_home=None):
    if not self.__hadoop_conf:
      try:
        self.__hadoop_conf = os.environ["HADOOP_CONF_DIR"]
      except KeyError:
        if self.cloudera():
          v = self.hadoop_version_info(hadoop_home)
          candidate = '/etc/hadoop-%d.%d/conf' % v[0:2]
        else:
          candidate = os.path.join(hadoop_home or self.hadoop_home(), 'conf')
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
        except IOError:
          pass
      self.__hadoop_params = params
    return self.__hadoop_params
