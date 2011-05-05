# BEGIN_COPYRIGHT
# END_COPYRIGHT

# DEV NOTE: this module is used by the setup script, so it MUST NOT
# rely on extension modules.

import os, re, subprocess


class HadoopVersionError(Exception):
  pass


def __is_exe(fpath):
  return os.path.exists(fpath) and os.access(fpath, os.X_OK)

def version_tuple(version_string):
  try:
    # sample version strings:  "0.20.3-cdh3", "0.20.2", "0.21.2"
    m = re.match(r"(\d+)\.(\d+)\.(\d+)(-(.+))?", version_string)
    vt = map(int, m.group(1,2,3))
    if m.group(5):
      vt = vt + [ m.group(5) ]
    vt = tuple(vt)
  except ValueError:
    raise HadoopVersionError("bad version string: %r" % version_string)
  return vt


def get_hadoop_version(hadoop_home):
  msg = "couldn't detect version for %r" % hadoop_home + ": %s"
  version = os.getenv("HADOOP_VERSION")
  if version:
    return version_tuple(version)
  hadoop_bin = get_hadoop_exec(hadoop_home)
  if not hadoop_bin:
    raise RuntimeError("Couldn't find hadoop executable in HADOOP_HOME/bin nor in your PATH.  Please adjust either of those variables.")
  args = [hadoop_bin, "version"]
  try:
    version = subprocess.Popen(
      args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
      ).communicate()[0].splitlines()[0].split()[-1]
  except (OSError, IndexError) as e:
    raise HadoopVersionError(msg % ("'%s %s' failed" % tuple(args)))
  else:
    return version_tuple(version)

def get_hadoop_exec(hadoop_home=None):
  # check whatever hadoop home the caller gave us
  if hadoop_home:
    hadoop = os.path.join(hadoop_home, "bin", "hadoop")
    if __is_exe(hadoop):
      return hadoop
  # check the environment's HADOOP_HOME
  if os.environ.has_key("HADOOP_HOME"):
    hadoop = os.path.join(os.environ["HADOOP_HOME"], "bin", "hadoop")
    if __is_exe(hadoop):
      return hadoop
  # search the PATH for hadoop
  for path in os.environ["PATH"].split(os.pathsep):
    hadoop = os.path.join(path, 'hadoop')
    if __is_exe(hadoop):
      return hadoop

  return None

