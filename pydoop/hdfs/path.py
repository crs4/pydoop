# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
pydoop.hdfs.path -- path name manipulations
-------------------------------------------
"""

import os, re

import fs as hdfs_fs
from config import DEFAULT_PORT, DEFAULT_USER


class _HdfsPathSplitter(object):

  PATTERN = re.compile(r"([a-z0-9+.-]+):(.*)")

  @classmethod
  def raise_bad_path(cls, hdfs_path, why=None):
    msg = "'%s' is not a valid HDFS path" % hdfs_path
    msg += " (%s)" % why if why else ""
    raise ValueError(msg)

  @classmethod
  def split(cls, hdfs_path, user):
    try:
      scheme, rest = cls.PATTERN.match(hdfs_path).groups()
    except AttributeError:
      scheme, rest = "hdfs", hdfs_path
    if scheme == "hdfs":
      if rest[:2] == "//" and rest[2] != "/":
        try:
          netloc, path = rest[2:].split("/", 1)
        except ValueError:
          cls.raise_bad_path(hdfs_path, "path part is empty")
        path = "/%s" % path
        try:
          hostname, port = netloc.split(":")
        except ValueError:
          hostname, port = netloc, DEFAULT_PORT
        try:
          port = int(port)
        except ValueError:
          cls.raise_bad_path(hdfs_path, "port must be an integer")
      elif rest[0] != "/":
        path = "/user/%s/%s" % (user, rest)
        hostname, port = "default", 0
      else:
        hostname, port, path = "default", 0, rest
      if ":" in path:
        cls.raise_bad_path(hdfs_path, "':' not allowed outside netloc part")
    elif scheme == "file":
      hostname, port, path = "", 0, rest
    else:
      cls.raise_bad_path(hdfs_path, "unsupported scheme %r" % scheme)
    path = "/%s" % path.lstrip("/")  # not handled by normpath
    return hostname, port, os.path.normpath(path)


def split(hdfs_path, user=None):
  """
  Split ``hdfs_path`` into a (hostname, port, path) tuple.

  :type hdfs_path: string
  :param hdfs_path: an HDFS path, e.g., ``hdfs://localhost:9000/user/me``
  :type user: string
  :param user: user name used to resolve relative paths, defaults to the
    current user
  :rtype: tuple
  :return: hostname, port, path
  """
  # Use a helper class to compile URL_PATTERN once and for all
  return _HdfsPathSplitter.split(hdfs_path, user or DEFAULT_USER)


def join(a, *p):
  """
  Join path name components, inserting ``/`` as needed.

  If any component looks like an absolute path (i.e., it starts with
  ``hdfs:`` or ``file:``), all previous components will be discarded.

  Note that this is *not* the reverse of :func:`split`, but rather a
  specialized version of os.path.join. It is the caller's
  responsibility to ensure that individual parts are correctly formed.
  """
  path = [a.rstrip("/")]
  for b in p:
    b = b.strip("/")
    if b.startswith('hdfs:') or b.startswith('file:'):
      path = [b]
    else:
      path.append(b)
  return "/".join(path)


def abspath(hdfs_path, user=None, local=False):
  """
  Return an absolute path for ``hdfs_path``.

  If ``local`` is true, it simply prepends 'file:' to
  ``os.path.abspath(hdfs_path)``. The ``user`` arg is passed to
  :func:`split`.
  """
  if local:
    return 'file:%s' % os.path.abspath(hdfs_path)
  hostname, port, path = split(hdfs_path, user=user)
  fs = hdfs_fs.hdfs(hostname, port)
  apath = join("hdfs://%s:%s" % (fs.host, fs.port), path)
  fs.close()
  return apath
