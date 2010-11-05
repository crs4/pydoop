# DEV NOTE: this module is used by the setup script, so it MUST NOT
# rely on other pydoop modules

import os, re, subprocess


class HadoopVersionError(Exception):
  pass


def get_hadoop_version(hadoop_home):
  msg = "couldn't detect version for %r" % hadoop_home + ": %s"
  hadoop_bin = os.path.join(hadoop_home, "bin/hadoop")
  if not os.path.exists(hadoop_bin):
    raise HadoopVersionError(msg % ("%r not found" % hadoop_bin))
  args = [hadoop_bin, "version"]
  try:
    version = subprocess.Popen(
      args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
      ).communicate()[0].splitlines()[0].split()[-1]
  except (OSError, IndexError) as e:
    raise HadoopVersionError(msg % ("'%s %s' failed" % tuple(args)))
  else:
    return tuple(map(int, version.split(".")))
