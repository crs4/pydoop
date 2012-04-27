# BEGIN_COPYRIGHT
# END_COPYRIGHT

# DEV NOTE: this module is used by the setup script, so it MUST NOT
# rely on extension modules.

import os, re, subprocess, glob

try:
  from config import DEFAULT_HADOOP_HOME
except ImportError:  # should only happen at compile time
  DEFAULT_HADOOP_HOME = None


class _EnvMonitor(object):

  ENV = os.environ.copy()

  @classmethod
  def has_changed(cls, var):
    """
    Return ``False`` if ``var`` has not changed since last check.

    If ``var`` has changed, return True if its current value is empty
    or ``None``, otherwise return the variable itself.
    """
    current_value = os.getenv(var)
    if current_value != cls.ENV.get(var):
      cls.ENV[var] = current_value
      return current_value or True
    return False


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


def get_hadoop_home(fallback=DEFAULT_HADOOP_HOME):
  hadoop_home = os.getenv("HADOOP_HOME")
  if hadoop_home:
    return hadoop_home
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
          hadoop_home = home_path
  if hadoop_home:
    return hadoop_home
  # search the PATH env var
  for path in os.environ["PATH"].split(os.pathsep):
    if is_exe(os.path.join(path, 'hadoop')):
      return os.path.dirname(path)
  # Try a few standard paths
  paths = sum([glob.glob(s) for s in (
    "/opt/hadoop*",
    "/usr/lib/hadoop*",
    "/usr/local/lib/hadoop*",
    )], [])
  if len(paths) > 0:
    hadoop_home = paths[0]
  if hadoop_home:
    return hadoop_home
  if fallback:
    return fallback
  raise ValueError("Hadoop home not found, try setting HADOOP_HOME")


def get_hadoop_version(hadoop_home=None):
  """
  Get the version for the Hadoop installation in ``hadoop_home``.

  See :func:`version_tuple` for the format of the return value.
  """
  err_msg = "could not determine Hadoop version, try setting HADOOP_VERSION"
  version = os.getenv("HADOOP_VERSION")
  if version:
    return version_tuple(version)
  else:
    try:
      hadoop = get_hadoop_exec(hadoop_home)
    except ValueError:
      raise ValueError(err_msg)
    args = [hadoop, "version"]
    try:
      version = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ).communicate()[0].splitlines()[0].split()[-1]
    except (OSError, IndexError):
      raise ValueError(err_msg)
    else:
      return version_tuple(version)


def get_hadoop_exec(hadoop_home=None):
  if hadoop_home:
    hadoop = os.path.join(hadoop_home, "bin", "hadoop")
    if is_exe(hadoop):
      return hadoop
  hadoop = os.path.join(get_hadoop_home(), "bin", "hadoop")
  if is_exe(hadoop):
    return hadoop
  raise ValueError("hadoop command not found, try setting HADOOP_HOME or PATH")


def cloudera_version(ver):
  if ver is not None and len(ver) > 3:
    cloudera_re = re.compile("cdh.*")
    return any(map(cloudera_re.match, ver[3:]))
  else:
    return False


def get_hadoop_conf(hadoop_home=None):
  hadoop_conf = os.getenv("HADOOP_CONF_DIR")
  if hadoop_conf:
    return hadoop_conf
  else:
    hadoop_version = get_hadoop_version(hadoop_home=hadoop_home)
    if cloudera_version(hadoop_version):
      candidate = '/etc/hadoop-%d.%d/conf' % hadoop_version[0:2]
      if os.path.isdir(candidate):
        return candidate
  if hadoop_home:
    candidate = os.path.join(hadoop_home, 'conf')
    if os.path.isdir(candidate):
      return candidate
  raise ValueError("Hadoop conf not found, try setting HADOOP_CONF_DIR")


class PathFinder(object):
  """
  Encapsulates the logic to find paths and other info required by Pydoop.
  """

  def __init__(self):
    self.__hadoop_home = None
    self.__hadoop_conf = None
    self.__hadoop_version = None

  def hadoop_home(self, fallback=DEFAULT_HADOOP_HOME):
    if _EnvMonitor.has_changed("HADOOP_HOME"):
      self.__hadoop_home = None
    if not self.__hadoop_home:
      self.__hadoop_home = get_hadoop_home(fallback=fallback)
    return self.__hadoop_home

  def hadoop_version(self):
    if (_EnvMonitor.has_changed("HADOOP_HOME") or
        _EnvMonitor.has_changed("HADOOP_VERSION")):
      self.__hadoop_version = None
    if not self.__hadoop_version:
      self.__hadoop_version = get_hadoop_version(self.hadoop_home())
    return self.__hadoop_version

  def hadoop_conf(self):
    if (_EnvMonitor.has_changed("HADOOP_HOME") or
        _EnvMonitor.has_changed("HADOOP_CONF_DIR")):
      self.__hadoop_conf = None
    if not self.__hadoop_conf:
      self.__hadoop_conf = get_hadoop_conf(self.hadoop_home())
    return self.__hadoop_conf

  def cloudera(self):
    return cloudera_version(self.hadoop_version())
