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

r"""
This module allows you to connect to an HDFS installation, read and
write files and get information on files, directories and global
filesystem properties.


Configuration
-------------

The hdfs module is built on top of ``libhdfs``, in turn a JNI wrapper
around the Java fs code: therefore, for the module to work properly,
the ``CLASSPATH`` environment variable must include all paths to the
relevant Hadoop jars. Pydoop will do this for you, but it needs to
know where your Hadoop installation is located and what is your hadoop
configuration directory: if Pydoop is not able to automatically find
these directories, you have to make sure that the ``HADOOP_HOME`` and
``HADOOP_CONF_DIR`` environment variables are set to the appropriate
values.

Another important environment variable for this module is
``LIBHDFS_OPTS``\ . This is used to set options for the JVM on top of
which the module runs, most notably the amount of memory it uses. If
``LIBHDFS_OPTS`` is not set, the C libhdfs will let it fall back to
the default for your system, typically 1 GB. According to our
experience, this is *much* more than most applications need and adds a
lot of unnecessary memory overhead. For this reason, the hdfs module
sets ``LIBHDFS_OPTS`` to ``-Xmx48m``\ , a value that we found to be
appropriate for most applications.  If your needs are different, you
can set the environment variable externally and it will override the
above setting.
"""

__all__ = [
    'path',
    'init',
    'reset',
    'hdfs',
    'default_is_local',
    'open',
    'dump',
    'load',
    'cp',
    'put',
    'get',
    'mkdir',
    'rmr',
    'lsl',
    'ls',
    'chmod',
    'move',
    'chown',
    'rename',
    'renames',
    'stat',
    'lstat',
    'access',
    'utime',
]


import os
import operator
from functools import reduce

import pydoop
from . import common, path
from pydoop.utils.py3compat import bintype

try:
    _ORIG_CLASSPATH
except NameError:
    _ORIG_CLASSPATH = os.getenv("CLASSPATH", "")


# --- MODULE CONFIG ---
def init():
    os.environ["CLASSPATH"] = "%s:%s:%s" % (
        pydoop.hadoop_classpath(), _ORIG_CLASSPATH, pydoop.hadoop_conf()
    )
    os.environ["LIBHDFS_OPTS"] = os.getenv(
        "LIBHDFS_OPTS", common.DEFAULT_LIBHDFS_OPTS
    ) + " -Djava.library.path=%s" % pydoop.hadoop_native()


init()


def reset():
    pydoop.reset()
    init()
# ---------------------


from .fs import hdfs, default_is_local


def open(hdfs_path, mode="r", buff_size=0, replication=0, blocksize=0,
         user=None, encoding=None, errors=None):
    """
    Open a file, returning an :class:`~.file.hdfs_file` object.

    ``hdfs_path`` and ``user`` are passed to :func:`~path.split`,
    while the other args are passed to the :class:`~.file.hdfs_file`
    constructor.
    """
    host, port, path_ = path.split(hdfs_path, user)
    fs = hdfs(host, port, user)
    return fs.open_file(path_, mode, buff_size, replication, blocksize,
                        encoding, errors)


def dump(data, hdfs_path, **kwargs):
    """\
    Write ``data`` to ``hdfs_path``.

    Keyword arguments are passed to :func:`open`, except for ``mode``,
    which is forced to ``"w"`` (or ``"wt"`` for text data).
    """
    kwargs["mode"] = "w" if isinstance(data, bintype) else "wt"
    with open(hdfs_path, **kwargs) as fo:
        i = 0
        bufsize = common.BUFSIZE
        while i < len(data):
            fo.write(data[i: i + bufsize])
            i += bufsize
    fo.fs.close()


def load(hdfs_path, **kwargs):
    """\
    Read the content of ``hdfs_path`` and return it.

    Keyword arguments are passed to :func:`open`. The `"mode"` kwarg
    must be readonly.
    """
    m, _ = common.parse_mode(kwargs.get("mode", "r"))
    if m != "r":
        raise ValueError("opening mode must be readonly")
    data = []
    with open(hdfs_path, **kwargs) as fi:
        bufsize = common.BUFSIZE
        while 1:
            chunk = fi.read(bufsize)
            if chunk:
                data.append(chunk)
            else:
                break
    fi.fs.close()
    return reduce(operator.add, data)


def _cp_file(src_fs, src_path, dest_fs, dest_path, **kwargs):
    kwargs.pop("mode", None)
    kwargs["mode"] = "r"
    with src_fs.open_file(src_path, **kwargs) as fi:
        kwargs["mode"] = "w"
        with dest_fs.open_file(dest_path, **kwargs) as fo:
            bufsize = common.BUFSIZE
            while 1:
                chunk = fi.read(bufsize)
                if chunk:
                    fo.write(chunk)
                else:
                    break


def cp(src_hdfs_path, dest_hdfs_path, **kwargs):
    """\
    Copy the contents of ``src_hdfs_path`` to ``dest_hdfs_path``.

    If ``src_hdfs_path`` is a directory, its contents will be copied
    recursively. Source file(s) are opened for reading and copies are
    opened for writing. Additional keyword arguments, if any, are
    handled like in :func:`open`.
    """
    src, dest = {}, {}
    try:
        for d, p in ((src, src_hdfs_path), (dest, dest_hdfs_path)):
            d["host"], d["port"], d["path"] = path.split(p)
            d["fs"] = hdfs(d["host"], d["port"])
        # --- does src exist? ---
        try:
            src["info"] = src["fs"].get_path_info(src["path"])
        except IOError:
            raise IOError("no such file or directory: %r" % (src["path"]))
        # --- src exists. Does dest exist? ---
        try:
            dest["info"] = dest["fs"].get_path_info(dest["path"])
        except IOError:
            if src["info"]["kind"] == "file":
                _cp_file(src["fs"], src["path"], dest["fs"], dest["path"],
                         **kwargs)
                return
            else:
                dest["fs"].create_directory(dest["path"])
                dest_hdfs_path = dest["fs"].get_path_info(dest["path"])["name"]
                for item in src["fs"].list_directory(src["path"]):
                    cp(item["name"], dest_hdfs_path, **kwargs)
                return
        # --- dest exists. Is it a file? ---
        if dest["info"]["kind"] == "file":
            raise IOError("%r already exists" % (dest["path"]))
        # --- dest is a directory ---
        dest["path"] = path.join(dest["path"], path.basename(src["path"]))
        if dest["fs"].exists(dest["path"]):
            raise IOError("%r already exists" % (dest["path"]))
        if src["info"]["kind"] == "file":
            _cp_file(src["fs"], src["path"], dest["fs"], dest["path"],
                     **kwargs)
        else:
            dest["fs"].create_directory(dest["path"])
            dest_hdfs_path = dest["fs"].get_path_info(dest["path"])["name"]
            for item in src["fs"].list_directory(src["path"]):
                cp(item["name"], dest_hdfs_path, **kwargs)
    finally:
        for d in src, dest:
            try:
                d["fs"].close()
            except KeyError:
                pass


def put(src_path, dest_hdfs_path, **kwargs):
    """\
    Copy the contents of ``src_path`` to ``dest_hdfs_path``.

    ``src_path`` is forced to be interpreted as an ordinary local path
    (see :func:`~path.abspath`). The source file is opened for reading
    and the copy is opened for writing. Additional keyword arguments,
    if any, are handled like in :func:`open`.
    """
    cp(path.abspath(src_path, local=True), dest_hdfs_path, **kwargs)


def get(src_hdfs_path, dest_path, **kwargs):
    """\
    Copy the contents of ``src_hdfs_path`` to ``dest_path``.

    ``dest_path`` is forced to be interpreted as an ordinary local
    path (see :func:`~path.abspath`). The source file is opened for
    reading and the copy is opened for writing. Additional keyword
    arguments, if any, are handled like in :func:`open`.
    """
    cp(src_hdfs_path, path.abspath(dest_path, local=True), **kwargs)


def mkdir(hdfs_path, user=None):
    """
    Create a directory and its parents as needed.
    """
    host, port, path_ = path.split(hdfs_path, user)
    fs = hdfs(host, port, user)
    retval = fs.create_directory(path_)
    fs.close()
    return retval


def rmr(hdfs_path, user=None):
    """
    Recursively remove files and directories.
    """
    host, port, path_ = path.split(hdfs_path, user)
    fs = hdfs(host, port, user)
    retval = fs.delete(path_)
    fs.close()
    return retval


def lsl(hdfs_path, user=None, recursive=False):
    """
    Return a list of dictionaries of file properties.

    If ``hdfs_path`` is a file, there is only one item corresponding to
    the file itself; if it is a directory and ``recursive`` is
    :obj:`False`, each list item corresponds to a file or directory
    contained by it; if it is a directory and ``recursive`` is
    :obj:`True`, the list contains one item for every file or directory
    in the tree rooted at ``hdfs_path``.
    """
    host, port, path_ = path.split(hdfs_path, user)
    fs = hdfs(host, port, user)
    if not recursive:
        dir_list = fs.list_directory(path_)
    else:
        treewalk = fs.walk(path_)
        top = next(treewalk)
        if top['kind'] == 'directory':
            dir_list = list(treewalk)
        else:
            dir_list = [top]
    fs.close()
    return dir_list


def ls(hdfs_path, user=None, recursive=False):
    """
    Return a list of hdfs paths.

    Works in the same way as :func:`lsl`, except for the fact that list
    items are hdfs paths instead of dictionaries of properties.
    """
    dir_list = lsl(hdfs_path, user, recursive)
    return [d["name"] for d in dir_list]


def chmod(hdfs_path, mode, user=None):
    """
    Change file mode bits.

    :type path: string
    :param path: the path to the file or directory
    :type mode: int
    :param mode: the bitmask to set it to (e.g., 0777)
    """
    host, port, path_ = path.split(hdfs_path, user)
    fs = hdfs(host, port, user)
    retval = fs.chmod(path_, mode)
    fs.close()
    return retval


def move(src, dest, user=None):
    """
    Move or rename src to dest.
    """
    src_host, src_port, src_path = path.split(src, user)
    dest_host, dest_port, dest_path = path.split(dest, user)
    src_fs = hdfs(src_host, src_port, user)
    dest_fs = hdfs(dest_host, dest_port, user)
    try:
        retval = src_fs.move(src_path, dest_fs, dest_path)
        return retval
    finally:
        src_fs.close()
        dest_fs.close()


def chown(hdfs_path, user=None, group=None, hdfs_user=None):
    """
    See :meth:`fs.hdfs.chown`.
    """
    user = user or ''
    group = group or ''
    host, port, path_ = path.split(hdfs_path, hdfs_user)
    with hdfs(host, port, hdfs_user) as fs:
        return fs.chown(path_, user=user, group=group)


def rename(from_path, to_path, user=None):
    """
    See :meth:`fs.hdfs.rename`.
    """
    fhost, fport, fpath = path.split(from_path, user)
    thost, tport, tpath = path.split(to_path, user)
    with hdfs(thost, tport, user) as fs:
        chost, cport = fs.host, fs.port
    with hdfs(fhost, fport, user) as fs:
        if fs.host != chost or fs.port != cport:
            raise RuntimeError("can't do a cross-fs rename")
        return fs.rename(fpath, tpath)


def renames(from_path, to_path, user=None):
    """
    Rename ``from_path`` to ``to_path``, creating parents as needed.
    """
    to_dir = path.dirname(to_path)
    if to_dir:
        mkdir(to_dir, user=user)
    rename(from_path, to_path, user=user)


# direct bindings
stat = path.stat
lstat = path.lstat
access = path.access
utime = path.utime
