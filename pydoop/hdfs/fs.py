# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
pydoop.hdfs.fs -- file system handles
-------------------------------------
"""

import os

import pydoop
hdfs_fs = pydoop.import_version_specific_module("_hdfs").hdfs_fs
from file import hdfs_file
import config


def _complain_ifclosed(closed):
  if closed:
    raise ValueError("I/O operation on closed HDFS instance")


class hdfs(hdfs_fs):
  """
  Represents a handle to an HDFS instance.

  :type host: string
  :param host: hostname or IP address of the HDFS NameNode. Set to
    an empty string (and ``port`` to 0) to connect to the local file
    system; Set to "default" (and ``port`` to 0) to connect to the
    "configured" file system.
  :type port: int
  :param port: the port on which the NameNode is listening
  :type user: string or ``None``
  :param user: the Hadoop domain user name. Defaults to the current
    UNIX user. Note that, in MapReduce applications, since tasks are
    spawned by the JobTracker, the default user will be the one that
    started the JobTracker itself.
  :type groups: list
  :param groups: ignored. Included for backwards compatibility.

  **Note:** when connecting to the local file system, ``user`` is
  ignored (i.e., it will always be the current UNIX user).
  """
  def __init__(self, host, port, user=None, groups=None):
    super(hdfs, self).__init__(host, port, user or "")
    self.closed = False
    try:
      h, p = self.get_path_info("/")["name"].rsplit("/", 2)[1].split(":")
    except ValueError:
      h, p = "", 0
    else:
      p = int(p)
    self.__host, self.__port = h, p

  @property
  def host(self):
    """
    The actual hdfs hostname (empty string for the local fs).
    """
    return self.__host

  @property
  def port(self):
    """
    The actual hdfs port (0 for the local fs).
    """
    return self.__port

  def open_file(self, path,
                flags=os.O_RDONLY,
                buff_size=0,
                replication=0,
                blocksize=0,
                readline_chunk_size=config.BUFSIZE):
    """
    Open an HDFS file.

    Pass 0 as buff_size, replication or blocksize if you want to use
    the default values, i.e., the ones set in the Hadoop configuration
    files.

    :type path: string
    :param path: the full path to the file
    :type flags: string or int
    :param flags: opening flags: ``'r'`` or :data:`os.O_RDONLY` for reading,
      ``'w'`` or :data:`os.O_WRONLY` for writing
    :type buff_size: int
    :param buff_size: read/write buffer size in bytes
    :type replication: int
    :param replication: HDFS block replication
    :type blocksize: int
    :param blocksize: HDFS block size
    :type readline_chunk_size: int
    :param readline_chunk_size: the amount of bytes that
      :meth:`hdfs_file.readline` will use for buffering
    :rtpye: :class:`hdfs_file`
    :return: handle to the open file
    """
    _complain_ifclosed(self.closed)
    path = str(path)  # the C API does not handle unicodes
    if flags == "r":
      flags = os.O_RDONLY
    elif flags == "w":
      flags = os.O_WRONLY
    if flags != os.O_RDONLY and flags != os.O_WRONLY:
      raise ValueError("opening mode %r not supported" % flags)
    return hdfs_file(super(hdfs, self).open_file(path, flags, buff_size,
                                                 replication, blocksize),
                     self, path, flags, readline_chunk_size)

  def capacity(self):
    """
    Return the raw capacity of the filesystem.
    
    :rtype: int
    :return: the raw capacity
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).capacity()

  def close(self):
    """
    Close the HDFS handle (disconnect).
    """
    if not self.closed:
      self.closed = True
      return super(hdfs, self).close()

  def copy(self, from_path, to_hdfs, to_path):
    """
    Copy file from one filesystem to another.

    :type from_path: string
    :param from_path: the path of the source file
    :type from_hdfs: :class:`hdfs`
    :param to_hdfs: the handle to destination filesystem
    :type to_path: string
    :param to_path: the path of the destination file
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).copy(from_path, to_hdfs, to_path)

  def create_directory(self, path):
    """
    Create directory ``path`` (non-existent parents will be created as well).

    :type path: string
    :param path: the path of the directory
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).create_directory(path)

  def default_block_size(self):
    """
    Get the default block size.
    
    :rtype: int
    :return: the default blocksize
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).default_block_size()
  
  def delete(self, path, recursive=True):
    """
    Delete ``path``.

    :type path: string
    :param path: the path of the file or directory
    :type recursive: bool
    :param recursive: if path is directory, delete it recursively when True;
      raise IOError when False and directory is non-empty
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).delete(path, recursive)

  def exists(self, path):
    """
    Check if a given path exists on the filesystem.

    :type path: string
    :param path: the path to look for
    :rtype: bool
    :return: True if ``path`` exists, else False
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).exists(path)

  def get_hosts(self, path, start, length):
    """
    Get hostnames where a particular block (determined by pos and
    blocksize) of a file is stored. Due to replication, a single block
    could be present on multiple hosts.

    :type path: string
    :param path: the path of the file
    :type start: int
    :param start: the start of the block
    :type length: int
    :param length: the length of the block
    :rtype: list
    :return: list of hosts that store the block
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).get_hosts(path, start, length)
  
  def get_path_info(self, path):
    """
    Get information about ``path`` as a dict of properties.
    
    :type path: string
    :param path: a path in the filesystem
    :rtype: dict
    :return: path information
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).get_path_info(path)

  def list_directory(self, path):
    """
    Get list of files and directories for ``path``\ .
    
    :type path: string
    :param path: the path of the directory
    :rtype: list
    :return: list of files and directories in ``path``
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).list_directory(path)
  
  def move(self, from_path, to_hdfs, to_path):
    """
    Move file from one filesystem to another.

    :type from_path: string
    :param from_path: the path of the source file
    :type from_hdfs: :class:`hdfs`
    :param to_hdfs: the handle to destination filesystem
    :type to_path: string
    :param to_path: the path of the destination file
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).move(from_path, to_hdfs, to_path)
    
  def rename(self, from_path, to_path):
    """
    Rename file.

    :type from_path: string
    :param from_path: the path of the source file
    :type to_path: string
    :param to_path: the path of the destination file    
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).rename(from_path, to_path)

  def set_replication(self, path, replication):
    """
    Set the replication of ``path`` to ``replication``\ .

    :type path: string
    :param path: the path of the file
    :type replication: int
    :param replication: the replication value
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).set_replication(path, replication)
  
  def set_working_directory(self, path):
    """
    Set the working directory to ``path``\ . All relative paths will
    be resolved relative to it.

    :type path: string
    :param path: the path of the directory
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).set_working_directory(path)

  def used(self):
    """
    Return the total raw size of all files in the filesystem.
    
    :rtype: int
    :return: total size of files in the file system
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).used()
  
  def working_directory(self):
    """
    Get the current working directory.
    
    :rtype: str
    :return: current working directory
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).working_directory()

  def chown(self, path, user='', group=''):
    """
    Change file owner and group.
    
    :type path: string
    :param path: the path to the file or directory
    :type user: string
    :param user: Hadoop username. Set to '' if only setting group
    :type group: string
    :param group: Hadoop group name. Set to '' if only setting user
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).chown(path, user, group)

  def chmod(self, path, mode):
    """
    Change file mode bits.
    
    :type path: string
    :param path: the path to the file or directory
    :type mode: int
    :param mode: the bitmask to set it to (e.g., 0777)
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).chmod(path, mode)

  def utime(self, path, mtime, atime):
    """
    Change file last access and modification times.

    :type path: string
    :param path: the path to the file or directory
    :type mtime: int
    :param mtime: new modification time in seconds
    :type atime: int
    :param atime: new access time in seconds
    """
    _complain_ifclosed(self.closed)
    return super(hdfs, self).utime(path, int(mtime), int(atime))
