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

"""
pydoop.hdfs.path -- Path Name Manipulations
-------------------------------------------
"""

import os
import re
import time

from . import common, fs as hdfs_fs
from pydoop.utils.py3compat import clong


curdir, pardir, sep = '.', '..', '/'  # pylint: disable=C0103


class StatResult(object):
    """
    Mimics the object type returned by :func:`os.stat`.

    Objects of this class are instantiated from dictionaries with the
    same structure as the ones returned by :meth:`~.fs.hdfs.get_path_info`.

    Attributes starting with ``st_`` have the same meaning as the
    corresponding ones in the object returned by :func:`os.stat`, although
    some of them may not make sense for an HDFS path (in this case,
    their value will be set to 0).  In addition, the ``kind``, ``name``
    and ``replication`` attributes are available, with the same values
    as in the input dict.
    """
    def __init__(self, path_info):
        self.st_mode = path_info['permissions']
        self.st_ino = 0
        self.st_dev = clong(0)
        self.st_nlink = 1
        self.st_uid = path_info['owner']
        self.st_gid = path_info['group']
        self.st_size = path_info['size']
        self.st_atime = path_info['last_access']
        self.st_mtime = path_info['last_mod']
        self.st_ctime = 0
        # --
        self.st_blksize = path_info['block_size']
        if self.st_blksize:
            n, r = divmod(path_info['size'], self.st_blksize)
            self.st_blocks = n + (r != 0)
        else:
            self.st_blocks = 0
        # --
        self.kind = path_info['kind']
        self.name = path_info['name']
        self.replication = path_info['replication']

    def __repr__(self):
        names = [_ for _ in dir(self) if _.startswith('st_')]
        names.extend(['kind', 'name', 'replication'])
        return '%s(%s)' % (
            self.__class__.__name__,
            ', '.join('%s=%r' % (_, getattr(self, _)) for _ in names)
        )


class _HdfsPathSplitter(object):

    PATTERN = re.compile(r"([a-z0-9+.-]+):(.*)")

    @classmethod
    def raise_bad_path(cls, hdfs_path, why=None):
        msg = "'%s' is not a valid HDFS path" % hdfs_path
        msg += " (%s)" % why if why else ""
        raise ValueError(msg)

    @classmethod
    def parse(cls, hdfs_path):
        if not hdfs_path:
            return "", "", ""
        try:
            scheme, rest = cls.PATTERN.match(hdfs_path).groups()
        except AttributeError:
            scheme, rest = "", hdfs_path
        if not rest:
            cls.raise_bad_path(hdfs_path, "no scheme-specific part")
        if rest.startswith("//") and not rest.startswith("///"):
            if not scheme:
                cls.raise_bad_path(hdfs_path, 'null scheme')
            try:
                netloc, path = rest[2:].split("/", 1)
                path = "/%s" % path
            except ValueError:
                netloc, path = rest[2:], ""
        elif scheme and not rest.startswith('/'):
            cls.raise_bad_path(hdfs_path, "relative path in absolute URI")
        else:
            netloc, path = "", rest
        if path.startswith("/"):
            path = "/%s" % path.lstrip("/")
        return scheme, netloc, path

    @classmethod
    def unparse(cls, scheme, netloc, path):
        hdfs_path = []
        if scheme:
            hdfs_path.append('%s:' % scheme.rstrip(':'))
        if netloc:
            if not scheme:
                raise ValueError('netloc provided, but scheme is empty')
            hdfs_path.append('//%s' % netloc)
        if hdfs_path and path and not path.startswith('/'):
            hdfs_path.append('/')
        hdfs_path.append(path)
        return ''.join(hdfs_path)

    @classmethod
    def split_netloc(cls, netloc):
        if not netloc:
            return "default", 0
        netloc = netloc.split(":")
        if len(netloc) > 2:
            raise ValueError("netloc is not well-formed: %r" % (netloc,))
        if len(netloc) < 2:
            return netloc[0], common.DEFAULT_PORT
        hostname, port = netloc
        try:
            port = int(port)
        except ValueError:
            raise ValueError(
                "bad netloc (port must be an integer): %r" % (netloc,)
            )
        return hostname, port

    @classmethod
    def split(cls, hdfs_path, user):
        if not hdfs_path:
            cls.raise_bad_path(hdfs_path, "empty")
        scheme, netloc, path = cls.parse(hdfs_path)
        if not scheme:
            scheme = "file" if hdfs_fs.default_is_local() else "hdfs"
        if scheme == "hdfs":
            if not path:
                cls.raise_bad_path(hdfs_path, "path part is empty")
            if ":" in path:
                cls.raise_bad_path(
                    hdfs_path, "':' not allowed outside netloc part"
                )
            hostname, port = cls.split_netloc(netloc)
            if not path.startswith("/"):
                path = "/user/%s/%s" % (user, path)
        elif scheme == "file":
            hostname, port, path = "", 0, netloc + path
        else:
            cls.raise_bad_path(hdfs_path, "unsupported scheme %r" % scheme)
        return hostname, port, path


def parse(hdfs_path):
    """
    Parse the given path and return its components.

    :type hdfs_path: str
    :param hdfs_path: an HDFS path, e.g., ``hdfs://localhost:9000/user/me``
    :rtype: tuple
    :return: scheme, netloc, path
    """
    return _HdfsPathSplitter.parse(hdfs_path)


def unparse(scheme, netloc, path):
    """
    Construct a path from its three components (see :func:`parse`).
    """
    return _HdfsPathSplitter.unparse(scheme, netloc, path)


def split(hdfs_path, user=None):
    """
    Split ``hdfs_path`` into a (hostname, port, path) tuple.

    :type hdfs_path: str
    :param hdfs_path: an HDFS path, e.g., ``hdfs://localhost:9000/user/me``
    :type user: str
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

    If any component is an absolute path (see :func:`isabs`), all
    previous components will be discarded.  However, full URIs (see
    :func:`isfull`) take precedence over incomplete ones:

    .. code-block:: python

      >>> import pydoop.hdfs.path as hpath
      >>> hpath.join('bar', '/foo')
      '/foo'
      >>> hpath.join('hdfs://host:1/', '/foo')
      'hdfs://host:1/foo'

    Note that this is *not* the reverse of :func:`split`, but rather a
    specialized version of :func:`os.path.join`. No check is made to determine
    whether the returned string is a valid HDFS path.
    """
    try:
        path = [parts[0]]
    except IndexError:
        raise TypeError("need at least one argument")
    for p in parts[1:]:
        path[-1] = path[-1].rstrip("/")
        full = isfull(path[0])
        if isfull(p) or (isabs(p) and not full):
            path = [p]
        else:
            path.append(p.lstrip('/'))
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

    Note that this function always return a full URI:

    .. code-block:: python

      >>> import pydoop.hdfs.path as hpath
      >>> hpath.abspath('/tmp')
      'hdfs://localhost:9000/tmp'
    """
    if local:
        return 'file:%s' % os.path.abspath(hdfs_path)
    if isfull(hdfs_path):
        return hdfs_path
    hostname, port, path = split(hdfs_path, user=user)
    if hostname:
        fs = hdfs_fs.hdfs(hostname, port)
        apath = join("hdfs://%s:%s" % (fs.host, fs.port), path)
        fs.close()
    else:
        apath = "file:%s" % os.path.abspath(path)
    return apath


def splitpath(hdfs_path):
    """
    Split ``hdfs_path`` into a (``head``, ``tail``) pair, according to
    the same rules as :func:`os.path.split`.
    """
    return (dirname(hdfs_path), basename(hdfs_path))


def basename(hdfs_path):
    """
    Return the final component of ``hdfs_path``.
    """
    return os.path.basename(hdfs_path)


def dirname(hdfs_path):
    """
    Return the directory component of ``hdfs_path``.
    """
    scheme, netloc, path = parse(hdfs_path)
    return unparse(scheme, netloc, os.path.dirname(path))


def exists(hdfs_path, user=None):
    """
    Return :obj:`True` if ``hdfs_path`` exists in the default HDFS.
    """
    hostname, port, path = split(hdfs_path, user=user)
    fs = hdfs_fs.hdfs(hostname, port)
    retval = fs.exists(path)
    fs.close()
    return retval


# -- libhdfs does not support fs.FileStatus.isSymlink() --
def lstat(hdfs_path, user=None):
    return stat(hdfs_path, user=user)


def lexists(hdfs_path, user=None):
    return exists(hdfs_path, user=user)
# --------------------------------------------------------


def kind(path, user=None):
    """
    Get the kind of item ("file" or "directory") that the path references.

    Return :obj:`None` if ``path`` doesn't exist.
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
    Return :obj:`True` if ``path`` refers to a directory.
    """
    return kind(path, user) == 'directory'


def isfile(path, user=None):
    """
    Return :obj:`True` if ``path`` refers to a file.
    """
    return kind(path, user) == 'file'


def expanduser(path):
    """
    Replace initial ``~`` or ``~user`` with the user's home directory.

    **NOTE:** if the default file system is HDFS, the ``~user`` form is
    expanded regardless of the user's existence.
    """
    if hdfs_fs.default_is_local():
        return os.path.expanduser(path)
    m = re.match(r'^~([^/]*)', path)
    if m is None:
        return path
    user = m.groups()[0] or common.DEFAULT_USER
    return '/user/%s%s' % (user, path[m.end(1):])


def expandvars(path):
    """
    Expand environment variables in ``path``.
    """
    return os.path.expandvars(path)


def _update_stat(st, path_):
    try:
        os_st = os.stat(path_)
    except OSError:
        pass
    else:
        for name in dir(os_st):
            if name.startswith('st_'):
                setattr(st, name, getattr(os_st, name))


def stat(path, user=None):
    """
    Performs the equivalent of :func:`os.stat` on ``path``, returning a
    :class:`StatResult` object.
    """
    host, port, path_ = split(path, user)
    fs = hdfs_fs.hdfs(host, port, user)
    retval = StatResult(fs.get_path_info(path_))
    if not host:
        _update_stat(retval, path_)
    fs.close()
    return retval


def getatime(path, user=None):
    """
    Get time of last access of ``path``.
    """
    return stat(path, user=user).st_atime


def getmtime(path, user=None):
    """
    Get time of last modification of ``path``.
    """
    return stat(path, user=user).st_mtime


def getctime(path, user=None):
    """
    Get time of creation / last metadata change of ``path``.
    """
    return stat(path, user=user).st_ctime


def getsize(path, user=None):
    """
    Get size, in bytes, of ``path``.
    """
    return stat(path, user=user).st_size


def isfull(path):
    """
    Return :obj:`True` if ``path`` is a full URI (starts with a scheme
    followed by a colon).

    No check is made to determine whether ``path`` is a valid HDFS path.
    """
    return bool(_HdfsPathSplitter.PATTERN.match(path))


def isabs(path):
    """
    Return :obj:`True` if ``path`` is absolute.

    A path is absolute if it is a full URI (see :func:`isfull`) or
    starts with a forward slash. No check is made to determine whether
    ``path`` is a valid HDFS path.
    """
    return isfull(path) or path.startswith('/')


def islink(path, user=None):
    """
    Return :obj:`True` if ``path`` is a symbolic link.

    Currently this function always returns :obj:`False` for non-local paths.
    """
    host, _, path_ = split(path, user)
    if host:
        return False  # libhdfs does not support fs.FileStatus.isSymlink()
    return os.path.islink(path_)


def ismount(path):
    """
    Return :obj:`True` if ``path`` is a mount point.

    This function always returns :obj:`False` for non-local paths.
    """
    host, _, path_ = split(path, None)
    if host:
        return False
    return os.path.ismount(path_)


def normcase(path):
    return path  # we only support Linux / OS X


def normpath(path):
    """
    Normalize ``path``, collapsing redundant separators and up-level refs.
    """
    scheme, netloc, path_ = parse(path)
    return unparse(scheme, netloc, os.path.normpath(path_))


def realpath(path):
    """
    Return ``path`` with symlinks resolved.

    Currently this function returns non-local paths unchanged.
    """
    scheme, netloc, path_ = parse(path)
    if scheme == 'file' or hdfs_fs.default_is_local():
        return unparse(scheme, netloc, os.path.realpath(path_))
    return path


def samefile(path1, path2, user=None):
    """
    Return :obj:`True` if both path arguments refer to the same path.
    """
    def tr(p):
        return abspath(normpath(realpath(p)), user=user)
    return tr(path1) == tr(path2)


def splitdrive(path):
    return '', path  # we only support Linux / OS X


def splitext(path):
    """
    Same as :func:`os.path.splitext`.
    """
    return os.path.splitext(path)


def access(path, mode, user=None):
    """
    Perform the equivalent of :func:`os.access` on ``path``.
    """
    scheme = parse(path)[0]
    if scheme == 'file' or hdfs_fs.default_is_local():
        return os.access(path, mode)
    if user is None:
        user = common.DEFAULT_USER
    st = stat(path)
    if st.st_uid == user:
        mode <<= 6
    else:
        try:
            groups = common.get_groups(user)
        except KeyError:
            # user isn't recognized on the system.  No group
            # information available
            groups = []
        if st.st_gid in groups:
            mode <<= 3
    return (st.st_mode & mode) == mode


def utime(hdfs_path, times=None, user=None):
    atime, mtime = times or 2 * (time.time(),)
    hostname, port, path = split(hdfs_path, user=user)
    with hdfs_fs.hdfs(hostname, port) as fs:
        fs.utime(path, mtime, atime)
