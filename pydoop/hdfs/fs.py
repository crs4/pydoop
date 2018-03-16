# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

"""
pydoop.hdfs.fs -- File System Handles
-------------------------------------
"""

import os
import socket
import getpass
import re
import operator as ops
import io

import pydoop
from . import common
from .file import FileIO, hdfs_file, local_file, TextIOWrapper
from .core import core_hdfs_fs

# py3 compatibility
from functools import reduce

from pydoop.utils.py3compat import basestring
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class _FSStatus(object):

    def __init__(self, fs, host, port, user, refcount=1):
        self.fs = fs
        self.host = host
        self.port = port
        self.user = user
        self.refcount = refcount

    def __repr__(self):
        return "_FSStatus(%s, %s)" % (self.fs, self.refcount)


def _complain_ifclosed(closed):
    if closed:
        raise ValueError("I/O operation on closed HDFS instance")


def _get_ip(host, default=None):
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror:
        ip = "0.0.0.0"  # same as socket.gethostbyname("")
    return ip if ip != "0.0.0.0" else default


def _get_connection_info(host, port, user):
    fs = core_hdfs_fs(host, port, user)
    res = urlparse(fs.get_working_directory())
    if res.scheme == "file":
        h, p, u = "", 0, getpass.getuser()
        fs.set_working_directory(os.getcwd())  # libhdfs "remembers" old cwd
    else:
        try:
            h, p = res.netloc.split(":")
        except ValueError:
            h, p = res.netloc, common.DEFAULT_PORT

            # try to find an IP address if we can't extract it from res.netloc
            if not res.netloc:
                hosts = fs.get_hosts(str(res.path), 0, 0)
                if hosts and hosts[0] and hosts[0][0]:
                    h, p = hosts[0][0], common.DEFAULT_PORT
        u = res.path.split("/", 2)[2]
    return h, int(p), u, fs


def default_is_local(hadoop_conf=None, hadoop_home=None):
    params = pydoop.hadoop_params(hadoop_conf, hadoop_home)
    default_fs = params.get('fs.default.name', params.get('fs.defaultFS', ''))
    return default_fs.startswith('file:')


class hdfs(object):
    """
    A handle to an HDFS instance.

    :type host: str
    :param host: hostname or IP address of the HDFS NameNode. Set to an
      empty string (and ``port`` to 0) to connect to the local file
      system; set to ``'default'`` (and ``port`` to 0) to connect to the
      default (i.e., the one defined in the Hadoop configuration files)
      file system.
    :type port: int
    :param port: the port on which the NameNode is listening
    :type user: str
    :param user: the Hadoop domain user name. Defaults to the current
      UNIX user. Note that, in MapReduce applications, since tasks are
      spawned by the JobTracker, the default user will be the one that
      started the JobTracker itself.
    :type groups: list
    :param groups: ignored. Included for backwards compatibility.

    **Note:** when connecting to the local file system, ``user`` is
    ignored (i.e., it will always be the current UNIX user).
    """
    _CACHE = {}
    _ALIASES = {"host": {}, "port": {}, "user": {}}

    def __canonize_hpu(self, hpu):
        host, port, user = hpu
        host = self._ALIASES["host"].get(host, host)
        port = self._ALIASES["port"].get(port, port)
        user = self._ALIASES["user"].get(user, user)
        return host, port, user

    def __lookup(self, hpu):
        if hpu[0]:
            hpu = self.__canonize_hpu(hpu)
        return self._CACHE[hpu]

    def __eq__(self, other):
        """
        :obj:`True` if ``self`` and ``other`` wrap the same Hadoop file
        system instance
        """
        return type(self) == type(other) and self.fs == other.fs

    def __init__(self, host="default", port=0, user=None, groups=None):
        host = host.strip()
        raw_host = host
        host = common.encode_host(host)
        if user is None:
            user = ""
        if not host:
            port = 0
            user = user or getpass.getuser()
        try:
            self.__status = self.__lookup((host, port, user))
        except KeyError:
            h, p, u, fs = _get_connection_info(host, port, user)
            aliasing_info = [] if user else [("user", u, user)]
            if h != "":
                aliasing_info.append(("port", p, port))
            ip = _get_ip(h, None)
            if ip:
                aliasing_info.append(("host", ip, h))
            else:
                ip = h
            aliasing_info.append(("host", ip, host))
            if raw_host != host:
                aliasing_info.append(("host", ip, raw_host))
            for k, true_x, x in aliasing_info:
                if true_x != x:
                    self._ALIASES[k][x] = true_x
            try:
                self.__status = self.__lookup((h, p, u))
            except KeyError:
                self.__status = _FSStatus(fs, h, p, u, refcount=0)
                self._CACHE[(ip, p, u)] = self.__status
        self.__status.refcount += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def fs(self):
        return self.__status.fs

    @property
    def refcount(self):
        return self.__status.refcount

    @property
    def host(self):
        """
        The actual hdfs hostname (empty string for the local fs).
        """
        return self.__status.host

    @property
    def port(self):
        """
        The actual hdfs port (0 for the local fs).
        """
        return self.__status.port

    @property
    def user(self):
        """
        The user associated with this HDFS connection.
        """
        return self.__status.user

    def close(self):
        """
        Close the HDFS handle (disconnect).
        """
        self.__status.refcount -= 1
        if self.refcount == 0:
            self.fs.close()
            for k, status in list(self._CACHE.items()):  # yes, we want a copy
                if status.refcount == 0:
                    del self._CACHE[k]

    @property
    def closed(self):
        return self.__status.refcount == 0

    def open_file(self, path,
                  mode="r",
                  buff_size=0,
                  replication=0,
                  blocksize=0,
                  encoding=None,
                  errors=None):
        """
        Open an HDFS file.

        Supported opening modes are "r", "w", "a". In addition, a
        trailing "t" can be added to specify text mode (e.g., "rt" =
        open for reading text).

        Pass 0 as ``buff_size``, ``replication`` or ``blocksize`` if you want
        to use the "configured" values, i.e., the ones set in the Hadoop
        configuration files.

        :type path: str
        :param path: the full path to the file
        :type mode: str
        :param mode: opening mode
        :type buff_size: int
        :param buff_size: read/write buffer size in bytes
        :type replication: int
        :param replication: HDFS block replication
        :type blocksize: int
        :param blocksize: HDFS block size
        :rtpye: :class:`~.file.hdfs_file`
        :return: handle to the open file

        """
        _complain_ifclosed(self.closed)
        if not path:
            raise ValueError("Empty path")
        m, is_text = common.parse_mode(mode)
        if not self.host:
            fret = local_file(self, path, m)
            if is_text:
                cls = io.BufferedReader if m == "r" else io.BufferedWriter
                fret = TextIOWrapper(cls(fret), encoding, errors)
            return fret
        f = self.fs.open_file(path, m, buff_size, replication, blocksize)
        cls = FileIO if is_text else hdfs_file
        fret = cls(f, self, mode)
        return fret

    def capacity(self):
        """
        Return the raw capacity of the filesystem.

        :rtype: int
        :return: filesystem capacity
        """
        _complain_ifclosed(self.closed)
        if self.__status.host is '':
            raise RuntimeError('Capacity is not defined for a local fs')
        return self.fs.get_capacity()

    def copy(self, from_path, to_hdfs, to_path):
        """
        Copy file from one filesystem to another.

        :type from_path: str
        :param from_path: the path of the source file
        :type to_hdfs: :class:`hdfs`
        :param to_hdfs: destination filesystem
        :type to_path: str
        :param to_path: the path of the destination file
        :raises: :exc:`~exceptions.IOError`
        """
        _complain_ifclosed(self.closed)
        if isinstance(to_hdfs, self.__class__):
            to_hdfs = to_hdfs.fs
        return self.fs.copy(from_path, to_hdfs, to_path)

    def create_directory(self, path):
        """
        Create directory ``path`` (non-existent parents will be created as
        well).

        :type path: str
        :param path: the path of the directory
        :raises: :exc:`~exceptions.IOError`
        """
        _complain_ifclosed(self.closed)
        return self.fs.create_directory(path)

    def default_block_size(self):
        """
        Get the default block size.

        :rtype: int
        :return: the default blocksize
        """
        _complain_ifclosed(self.closed)
        return self.fs.get_default_block_size()

    def delete(self, path, recursive=True):
        """
        Delete ``path``.

        :type path: str
        :param path: the path of the file or directory
        :type recursive: bool
        :param recursive: if ``path`` is a directory, delete it recursively
          when :obj:`True`
        :raises: :exc:`~exceptions.IOError` when ``recursive`` is
          :obj:`False` and directory is non-empty
        """
        _complain_ifclosed(self.closed)
        return self.fs.delete(path, recursive)

    def exists(self, path):
        """
        Check if a given path exists on the filesystem.

        :type path: str
        :param path: the path to look for
        :rtype: bool
        :return: :obj:`True` if ``path`` exists
        """
        _complain_ifclosed(self.closed)
        return self.fs.exists(path)

    def get_hosts(self, path, start, length):
        """
        Get hostnames where a particular block (determined by pos and
        blocksize) of a file is stored. Due to replication, a single block
        could be present on multiple hosts.

        :type path: str
        :param path: the path of the file
        :type start: int
        :param start: the start of the block
        :type length: int
        :param length: the length of the block
        :rtype: list
        :return: list of hosts that store the block
        """
        _complain_ifclosed(self.closed)
        return self.fs.get_hosts(path, start, length)

    def get_path_info(self, path):
        """
        Get information about ``path`` as a dict of properties.

        The return value, based upon ``fs.FileStatus`` from the Java API,
        has the following fields:

        * ``block_size``: HDFS block size of ``path``
        * ``group``: group associated with ``path``
        * ``kind``: ``'file'`` or ``'directory'``
        * ``last_access``: last access time of ``path``
        * ``last_mod``: last modification time of ``path``
        * ``name``: fully qualified path name
        * ``owner``: owner of ``path``
        * ``permissions``: file system permissions associated with ``path``
        * ``replication``: replication factor of ``path``
        * ``size``: size in bytes of ``path``

        :type path: str
        :param path: a path in the filesystem
        :rtype: dict
        :return: path information
        :raises: :exc:`~exceptions.IOError`
        """
        _complain_ifclosed(self.closed)
        return self.fs.get_path_info(path)

    def list_directory(self, path):
        r"""
        Get list of files and directories for ``path``\ .

        :type path: str
        :param path: the path of the directory
        :rtype: list
        :return: list of files and directories in ``path``
        :raises: :exc:`~exceptions.IOError`
        """
        _complain_ifclosed(self.closed)
        return self.fs.list_directory(path)

    def move(self, from_path, to_hdfs, to_path):
        """
        Move file from one filesystem to another.

        :type from_path: str
        :param from_path: the path of the source file
        :type from_hdfs: :class:`hdfs`
        :param to_hdfs: destination filesystem
        :type to_path: str
        :param to_path: the path of the destination file
        :raises: :exc:`~exceptions.IOError`
        """
        _complain_ifclosed(self.closed)
        if isinstance(to_hdfs, self.__class__):
            to_hdfs = to_hdfs.fs
        return self.fs.move(from_path, to_hdfs, to_path)

    def rename(self, from_path, to_path):
        """
        Rename file.

        :type from_path: str
        :param from_path: the path of the source file
        :type to_path: str
        :param to_path: the path of the destination file
        :raises: :exc:`~exceptions.IOError`
        """
        _complain_ifclosed(self.closed)
        return self.fs.rename(from_path, to_path)

    def set_replication(self, path, replication):
        r"""
        Set the replication of ``path`` to ``replication``\ .

        :type path: str
        :param path: the path of the file
        :type replication: int
        :param replication: the replication value
        :raises: :exc:`~exceptions.IOError`
        """
        _complain_ifclosed(self.closed)
        return self.fs.set_replication(path, replication)

    def set_working_directory(self, path):
        r"""
        Set the working directory to ``path``\ . All relative paths will
        be resolved relative to it.

        :type path: str
        :param path: the path of the directory
        :raises: :exc:`~exceptions.IOError`
        """
        _complain_ifclosed(self.closed)
        return self.fs.set_working_directory(path)

    def used(self):
        """
        Return the total raw size of all files in the filesystem.

        :rtype: int
        :return: total size of files in the file system
        """
        _complain_ifclosed(self.closed)
        return self.fs.get_used()

    def working_directory(self):
        """
        Get the current working directory.

        :rtype: str
        :return: current working directory
        """
        _complain_ifclosed(self.closed)
        wd = self.fs.get_working_directory()
        return wd

    def chown(self, path, user='', group=''):
        """
        Change file owner and group.

        :type path: str
        :param path: the path to the file or directory
        :type user: str
        :param user: Hadoop username. Set to '' if only setting group
        :type group: str
        :param group: Hadoop group name. Set to '' if only setting user
        :raises: :exc:`~exceptions.IOError`
        """
        _complain_ifclosed(self.closed)
        return self.fs.chown(path, user, group)

    @staticmethod
    def __get_umask():
        current_umask = os.umask(0)
        os.umask(current_umask)
        return current_umask

    def __compute_mode_from_string(self, path, mode_string):
        """
        Scan a unix-style mode string and apply it to ``path``.

        :type mode_string: str
        :param mode_string: see ``man chmod`` for details. ``X``, ``s``
          and ``t`` modes are not supported.  The string should match the
          following regular expression: ``[ugoa]*[-+=]([rwx]*)``.
        :rtype: int
        :return: a new mode integer resulting from applying ``mode_string``
          to ``path``.
        :raises: :exc:`~exceptions.ValueError` if ``mode_string`` is invalid.
        """
        Char_to_perm_byte = {'r': 4, 'w': 2, 'x': 1}
        Fields = (('u', 6), ('g', 3), ('o', 0))
        # --
        m = re.match(r"\s*([ugoa]*)([-+=])([rwx]*)\s*", mode_string)
        if not m:
            raise ValueError("Invalid mode string %s" % mode_string)
        who = m.group(1)
        what_op = m.group(2)
        which_perm = m.group(3)
        # --
        old_mode = self.fs.get_path_info(path)['permissions']
        # The mode to be applied by the operation, repeated three
        # times in a list, for user, group, and other respectively.
        # Initially these are identical, but some may change if we
        # have to respect the umask setting.
        op_perm = [
            reduce(ops.ior, [Char_to_perm_byte[c] for c in which_perm])
        ] * 3
        if 'a' in who:
            who = 'ugo'
        elif who == '':
            who = 'ugo'
            # erase the umask bits
            inverted_umask = ~self.__get_umask()
            for i, field in enumerate(Fields):
                op_perm[i] &= (inverted_umask >> field[1]) & 0x7
        # for each user, compute the permission bit and set it in the mode
        new_mode = 0
        for i, tpl in enumerate(Fields):
            field, shift = tpl
            # shift by the bits specified for the field; keep only the
            # 3 lowest bits
            old = (old_mode >> shift) & 0x7
            if field in who:
                if what_op == '-':
                    new = old & ~op_perm[i]
                elif what_op == '=':
                    new = op_perm[i]
                elif what_op == '+':
                    new = old | op_perm[i]
                else:
                    raise RuntimeError(
                        "unexpected permission operation %s" % what_op
                    )
            else:
                # copy the previous permissions
                new = old
            new_mode |= new << shift
        return new_mode

    def chmod(self, path, mode):
        """
        Change file mode bits.

        :type path: str
        :param path: the path to the file or directory
        :type mode: int
        :param mode: the bitmask to set it to (e.g., 0777)
        :raises: :exc:`~exceptions.IOError`
        """
        _complain_ifclosed(self.closed)
        try:
            return self.fs.chmod(path, mode)
        except TypeError:
            mode = self.__compute_mode_from_string(path, mode)
            return self.fs.chmod(path, mode)

    def utime(self, path, mtime, atime):
        """
        Change file last access and modification times.

        :type path: str
        :param path: the path to the file or directory
        :type mtime: int
        :param mtime: new modification time in seconds
        :type atime: int
        :param atime: new access time in seconds
        :raises: :exc:`~exceptions.IOError`
        """
        _complain_ifclosed(self.closed)
        return self.fs.utime(path, int(mtime), int(atime))

    def walk(self, top):
        """
        Generate infos for all paths in the tree rooted at ``top`` (included).

        The ``top`` parameter can be either an HDFS path string or a
        dictionary of properties as returned by :meth:`get_path_info`.

        :type top: str, dict
        :param top: an HDFS path or path info dict
        :rtype: iterator
        :return: path infos of files and directories in the tree rooted at
          ``top``
        :raises: :exc:`~exceptions.IOError`; :exc:`~exceptions.ValueError`
          if ``top`` is empty
        """
        if not top:
            raise ValueError("Empty path")
        if isinstance(top, basestring):
            top = self.get_path_info(top)
        yield top
        if top['kind'] == 'directory':
            for info in self.list_directory(top['name']):
                for item in self.walk(info):
                    yield item
