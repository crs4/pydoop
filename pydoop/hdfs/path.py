# BEGIN_COPYRIGHT
# 
# Copyright 2009-2013 CRS4.
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

"""
pydoop.hdfs.path -- Path Name Manipulations
-------------------------------------------
"""

import os, re

import common
import fs as hdfs_fs


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
      rest = hdfs_path
      scheme = "file" if hdfs_fs.default_is_local() else "hdfs"
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
          hostname, port = netloc, common.DEFAULT_PORT
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
    if path.startswith("/"):
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
  return _HdfsPathSplitter.split(hdfs_path, user or common.DEFAULT_USER)


def join(*parts):
  """
  Join path name components, inserting ``/`` as needed.

  If any component looks like an absolute path (i.e., it starts with
  ``hdfs:`` or ``file:``), all previous components will be discarded.

  Note that this is *not* the reverse of :func:`split`, but rather a
  specialized version of os.path.join. No check is made to determine
  whether the returned string is a valid HDFS path.
  """
  try:
    path = [parts[0].rstrip("/")]
  except IndexError:
    raise TypeError("need at least one argument")
  for p in parts[1:]:
    p = p.strip("/")
    if p.startswith('hdfs:') or p.startswith('file:'):
      path = [p]
    else:
      path.append(p)
  return "/".join(path)


def abspath(hdfs_path, user=None, local=False):
  """
  Return an absolute path for ``hdfs_path``.

  The ``user`` arg is passed to :func:`split`. The ``local`` argument
  forces ``hdfs_path`` to be interpreted as an ordinary local path:

  .. code-block:: python

    >>> import os
    >>> os.chdir('/tmp')
    >>> import pydoop.hdfs.path as hpath
    >>> hpath.abspath('file:/tmp')
    'file:/tmp'
    >>> hpath.abspath('file:/tmp', local=True)
    'file:/tmp/file:/tmp'
  """
  if local:
    return 'file:%s' % os.path.abspath(hdfs_path)
  if _HdfsPathSplitter.PATTERN.match(hdfs_path):
    return hdfs_path
  hostname, port, path = split(hdfs_path, user=user)
  if hostname:
    fs = hdfs_fs.hdfs(hostname, port)
    apath = join("hdfs://%s:%s" % (fs.host, fs.port), path)
    fs.close()
  else:
    apath = "file:%s" % os.path.abspath(path)
  return apath


# basename/dirname: we only support Linux, so it's OK to use os.path
def basename(hdfs_path):
  """
  Return the final component of ``hdfs_path``.
  """
  return os.path.basename(hdfs_path)


def dirname(hdfs_path):
  """
  Return the directory component of ``hdfs_path``.
  """
  return os.path.dirname(hdfs_path)


def exists(hdfs_path, user=None):
  """
  Return ``True`` if ``hdfs_path`` exists in the default HDFS, else ``False``.
  """
  hostname, port, path = split(hdfs_path, user=user)
  fs = hdfs_fs.hdfs(hostname, port)
  retval = fs.exists(path)
  fs.close()
  return retval


def kind(path, user=None):
  """
  Get the kind of item that the path references.

  Return None if the path doesn't exist.
  """
  hostname, port, path = split(path, user=user)
  fs = hdfs_fs.hdfs(hostname, port)
  try:
    return fs.get_path_info(path)['kind']
  except IOError:
    return None
  finally:
    fs.close()


def isdir(path, user=None):
  """
  Return True if ``path`` refers to a directory; False otherwise.
  """
  return kind(path, user) == 'directory'


def isfile(path, user=None):
  """
  Return True if ``path`` refers to a file; False otherwise.
  """
  return kind(path, user) == 'file'
