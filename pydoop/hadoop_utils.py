# BEGIN_COPYRIGHT
# END_COPYRIGHT

# DEV NOTE: this module is used by the setup script, so it MUST NOT
# rely on extension modules.

import os, re, subprocess, glob

try:
  _ORIG_HADOOP_HOME
except NameError:
  _ORIG_HADOOP_HOME = os.getenv("HADOOP_HOME")
try:
  _ORIG_HADOOP_CONF_DIR
except NameError:
  _ORIG_HADOOP_CONF_DIR = os.getenv("HADOOP_CONF_DIR")
try:
  from config import DEFAULT_HADOOP_HOME
except ImportError:  # should only happen at compile time
  DEFAULT_HADOOP_HOME = None


class HadoopVersionError(Exception):
  pass


def is_exe(fpath):
  return os.path.exists(fpath) and os.access(fpath, os.X_OK)

def is_readable(fpath):
  return os.path.exists(fpath) and os.access(fpath, os.R_OK)


def version_tuple(version_string):
  """
  Break a version string into its components.

  The first 3 elements of the tuple are converted to integers and represent
  the major, minor, and bugfix Hadoop version numbers.  Subsequent elements,
  if they exist, are other various appendages (e.g., SNAPSHOT, cdh3, etc.).

  raises HadoopVersionError if the version string is in an unrecognized format.
  """
  # sample version strings: "0.20.3-cdh3", "0.20.2", "0.21.2",
  # '0.20.203.1-SNAPSHOT'
  error_msg = "unrecognized version string format: %r" % version_string
  if not re.match(r"(\d+)(\.\d+)*(-.+)?", version_string):
    raise HadoopVersionError(error_msg)
  parts = re.split('[.-]', version_string)
  if len(parts) < 3:
    raise HadoopVersionError(error_msg)
  try:
    vt = map(int, parts[0:3])
    if len(parts) > 3:
      vt = vt + parts[3:]
    vt = tuple(vt)
  except ValueError:
    raise HadoopVersionError(error_msg)
  return vt


def get_hadoop_version(hadoop_home=None):
  """
  Get the version for the Hadoop installation in ``hadoop_home``.

  If ``hadoop_home`` is None, tries to execute ``hadoop version``
  and scans its output to detect the version number (see
  :func:`version_tuple` for the format of the return value).
  """
  msg = "couldn't detect version for %r" % hadoop_home + ": %s"
  version = os.getenv("HADOOP_VERSION")
  if version:
    return version_tuple(version)
  hadoop_bin = get_hadoop_exec(hadoop_home)
  if not hadoop_bin:
    raise RuntimeError(
      "Couldn't find hadoop executable in HADOOP_HOME/bin nor in your PATH. " +
      "Please adjust either of those variables."
      )
  args = [hadoop_bin, "version"]
  try:
    version = subprocess.Popen(
      args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
      ).communicate()[0].splitlines()[0].split()[-1]
  except (OSError, IndexError):
    raise HadoopVersionError(msg % ("'%s %s' failed" % tuple(args)))
  else:
    return version_tuple(version)


def get_hadoop_exec(hadoop_home=None):
  # check whatever hadoop home the caller gave us
  if hadoop_home:
    hadoop = os.path.join(hadoop_home, "bin", "hadoop")
    if is_exe(hadoop):
      return hadoop
  # check the environment's HADOOP_HOME
  if os.environ.has_key("HADOOP_HOME"):
    hadoop = os.path.join(os.environ["HADOOP_HOME"], "bin", "hadoop")
    if is_exe(hadoop):
      return hadoop
  # search the PATH for hadoop
  for path in os.environ["PATH"].split(os.pathsep):
    hadoop = os.path.join(path, 'hadoop')
    if is_exe(hadoop):
      return hadoop
  return None


class PathFinder(object):
  """
  Encapsulates the logic to find paths and other info required by Pydoop.
  """

  def __init__(self):
    self.__hadoop_home = None
    self.__hadoop_conf = None
    self.__hadoop_version = None
    self.__initialized = False

  def hadoop_home(self, fallback=DEFAULT_HADOOP_HOME):
    if os.getenv("HADOOP_HOME") != _ORIG_HADOOP_HOME:
      self.__initialized = False
    if not self.__initialized:
      self.__init_paths()
    if self.__hadoop_home is None:
      if fallback:
        self.__hadoop_home = fallback
      else:
        raise ValueError("HADOOP_HOME not set")
    return self.__hadoop_home

  def hadoop_version(self):
    if os.getenv("HADOOP_HOME") != _ORIG_HADOOP_HOME:
      self.__initialized = False
    if not self.__initialized:
      self.__init_paths()
    if self.__hadoop_version is None:
      raise HadoopVersionError("Could not determine Hadoop version")
    return self.__hadoop_version

  def hadoop_conf(self):
    if (os.getenv("HADOOP_HOME") != _ORIG_HADOOP_HOME or
        os.getenv("HADOOP_CONF_DIR") != _ORIG_HADOOP_CONF_DIR):
      self.__initialized = False
    if not self.__initialized:
      self.__init_paths()
    if self.__hadoop_conf is None:
      raise ValueError("HADOOP_CONF_DIR not set")
    return self.__hadoop_conf

  def cloudera(self):
    return self.__cloudera_version(self.hadoop_version())

  @staticmethod
  def __cloudera_version(ver):
    if ver is not None and len(ver) > 3:
      cloudera_re = re.compile("cdh.*")
      return any(map(cloudera_re.match, ver[3:]))
    else:
      return False

  def __init_paths(self):
    self.__hadoop_home = None
    self.__hadoop_conf = None
    self.__hadoop_version = None
    #--- HADOOP_HOME ---
    self.__hadoop_home = os.getenv("HADOOP_HOME")
    if not self.__hadoop_home:
      # look in hadoop config files, under /etc/default.  In the
      # hadoop-0.20 package from cloudera this is called hadoop-0.20.
      # I don't know how they'll handle later versions (e.g.,
      # 0.20.203, 1.0.1, etc.).  Conflicting packages?  More version
      # appendages?  For now we'll just grab the first of the sorted
      # list of versions (should be the newest).
      hadoop_cfg_files = sorted(glob.glob("/etc/default/hadoop*"), reverse=True)
      if hadoop_cfg_files:
        with open(hadoop_cfg_files[0]) as f:
          # get HADOOP_HOME directories from this file.  If there's
          # more than one we keep the last one.
          dirs = [line.rstrip("\n").split('=', 1) for line in f
                  if re.match(r"\s*HADOOP_HOME=.*", line)]
          if len(dirs) > 0:
            home_path = dirs[-1]
            if os.path.isdir(home_path):
              self.__hadoop_home = home_path
    if not self.__hadoop_home:
      # Try a few standard paths, then give up
      paths = sum([glob.glob(s) for s in (
        "/opt/hadoop*",
        "/usr/lib/hadoop*",
        "/usr/local/lib/hadoop*",
        )], [])
      if len(paths) > 0:
        self.__hadoop_home = paths[0]
    #--- HADOOP_VERSION ---
    try:
      self.__hadoop_version = get_hadoop_version(self.__hadoop_home)
    except (HadoopVersionError, RuntimeError):
      pass  # leave self.hadoop_version as None
    #--- HADOOP_CONF_DIR ---
    if "HADOOP_CONF_DIR" in os.environ:
      self.__hadoop_conf = os.environ["HADOOP_CONF_DIR"]
    elif self.__cloudera_version(self.__hadoop_version):
      candidate = '/etc/hadoop-%d.%d/conf' % self.__hadoop_version[0:2]
      if os.path.isdir(candidate):
        self.__hadoop_conf = candidate
    if self.__hadoop_conf is None:
      # Try in hadoop home, then give up
      if self.__hadoop_home is not None:
        candidate = os.path.join(self.__hadoop_home, 'conf')
        if os.path.isdir(candidate):
          self.__hadoop_conf = candidate
    self.__initialized = True
