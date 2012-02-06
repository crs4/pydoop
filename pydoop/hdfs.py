# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
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
configuration directory.

In practice, what you need to do is make sure that the ``HADOOP_HOME``
and the ``HADOOP_CONF_DIR`` (unless it coincides with
``${HADOOP_HOME}/conf``\ ) environment variables are correctly set
according to your installation. If ``HADOOP_HOME`` is not set or
empty, the hdfs module will raise an exception; if ``HADOOP_CONF_DIR``
is not set or empty, it will fall back to ``${HADOOP_HOME}/conf``\ .

Another important environment variable for this module is
``LIBHDFS_OPTS``\ . This is used to set options for the JVM on top of
which the module runs, most notably the amount of memory it uses. If
``LIBHDFS_OPTS`` is not set, the C libhdfs will let it fall back to
the default for your system, typically 1 GB. According to our
experience, this is *much* more than most applications need and adds a
lot of unnecessary memory overhead. For this reason, the hdfs module
sets ``LIBHDFS_OPTS`` to ``-Xmx48m``\ , a value that we found to be
appropriate for most applications. If your needs are different, you
can set the environment variable externally and it will override the
above setting.
"""

import os, glob

import pydoop

DEFAULT_LIBHDFS_OPTS = "-Xmx48m"  # enough for most applications.

HADOOP_CONF_DIR = pydoop.hadoop_conf()

jars = glob.glob(os.path.join(pydoop.hadoop_home(), "lib/*.jar"))
jars.extend(glob.glob(os.path.join(pydoop.hadoop_home(), "hadoop*.jar")))
jars.append(HADOOP_CONF_DIR)

CLASSPATH = os.environ.setdefault("CLASSPATH", "")
CLASSPATH = "%s:%s" % (":".join(jars), CLASSPATH)

os.environ["CLASSPATH"] = CLASSPATH
os.environ["LIBHDFS_OPTS"] = os.getenv("LIBHDFS_OPTS", DEFAULT_LIBHDFS_OPTS)

hdfs_fs = pydoop.import_version_specific_module("_hdfs").hdfs_fs

from utils import split_hdfs_path


def _complain_ifclosed(closed):
  if closed:
    raise ValueError("I/O operation on closed object")


class hdfs_file(object):
  """
  Instances of this class represent HDFS file objects.

  Objects from this class should not be instantiated directly.  The
  preferred way to open an HDFS file is with the :func:`open` function;
  alternatively, :meth:`hdfs.open_file` can be used.
  """
  
  DEFAULT_CHUNK_SIZE = 16384
  ENDL = os.linesep

  def __init__(self, raw_hdfs_file, fs, name, flags,
               chunk_size=DEFAULT_CHUNK_SIZE):
    if not chunk_size > 0:
      raise ValueError("chunk size must be positive")
    self.f = raw_hdfs_file
    self.__fs = fs
    self.__name = fs.get_path_info(name)["name"]
    self.__size = fs.get_path_info(name)["size"]
    self.__mode = "r" if flags == os.O_RDONLY else "w"
    self.chunk_size = chunk_size
    self.closed = False
    self.__reset()

  def __reset(self):
    self.buffer_list = []
    self.chunk = ""
    self.EOF = False
    self.p = 0

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.close()

  @property
  def fs(self):
    """
    The file's hdfs instance.
    """
    return self.__fs

  @property
  def name(self):
    """
    The file's fully qualified name.
    """
    return self.__name

  @property
  def size(self):
    """
    The file's size in bytes. This attribute is initialized when the
    file is opened and updated when it is closed.
    """
    return self.__size

  @property
  def mode(self):
    """
    The I/O mode for the file.
    """
    return self.__mode

  def __read_chunk(self):
    self.chunk = self.f.read(self.chunk_size)
    self.p = 0
    if not self.chunk:
      self.EOF = True

  def __read_chunks_until_nl(self):
    if self.EOF:
      eol = self.chunk.find(self.ENDL, self.p)
      return eol if eol > -1 else len(self.chunk)
    if not self.chunk:
      self.__read_chunk()
    eol = self.chunk.find(self.ENDL, self.p)
    i = 0
    while eol < 0 and not self.EOF:
      i += 1
      if self.p < len(self.chunk):
        self.buffer_list.append(self.chunk[self.p:])
      self.__read_chunk()
      eol = self.chunk.find(self.ENDL, self.p)
    return eol if eol > -1 else len(self.chunk)

  def readline(self):
    """
    Read and return a line of text.

    :rtype: string
    :return: the next line of text in the file, including the newline character
    """
    _complain_ifclosed(self.closed)
    eol = self.__read_chunks_until_nl()
    line = "".join(self.buffer_list) + self.chunk[self.p:eol+1]
    self.buffer_list = []
    self.p = eol+1
    return line

  def next(self):
    """
    Return the next input line, or raise :class:`StopIteration`
    when EOF is hit.
    """
    _complain_ifclosed(self.closed)
    line = self.readline()
    if line == "":
      raise StopIteration
    return line

  def __iter__(self):
    return self
  
  def available(self):
    """
    Number of bytes that can be read from this input stream without blocking.

    :rtype: int
    :return: available bytes
    """
    _complain_ifclosed(self.closed)
    return self.f.available()
  
  def close(self):
    """
    Close the file.
    """
    if not self.closed:
      self.closed = True
      retval = self.f.close()
      if self.mode == "w":
        self.__size = self.fs.get_path_info(self.name)["size"]
      return retval
  
  def pread(self, position, length):
    """
    Read ``length`` bytes of data from the file, starting from ``position``\ .

    :type position: int
    :param position: position from which to read
    :type length: int
    :param length: the number of bytes to read
    :rtype: string
    :return: the chunk of data read from the file
    """
    _complain_ifclosed(self.closed)
    return self.f.pread(position, length)
  
  def pread_chunk(self, position, chunk):
    """
    Works like :meth:`pread`\ , but data is stored in the writable
    buffer ``chunk`` rather than returned. Reads at most a number of
    bytes equal to the size of ``chunk``\ .

    :type position: int
    :param position: position from which to read
    :type chunk: writable string buffer
    :param chunk: a c-like string buffer, such as the one returned by the
      ``create_string_buffer`` function in the :mod:`ctypes` module
    :rtype: int
    :return: the number of bytes read
    """
    _complain_ifclosed(self.closed)
    return self.f.pread_chunk(position, chunk)
  
  def read(self, length=-1):
    """
    Read ``length`` bytes from the file.  If ``length`` is negative or
    omitted, read all data until EOF.

    :type length: int
    :param length: the number of bytes to read
    :rtype: string
    :return: the chunk of data read from the file
    """
    _complain_ifclosed(self.closed)
    # NOTE: libhdfs read stops at block boundaries: it is *essential*
    # to ensure that we actually read the required number of bytes.
    if length < 0:
      length = self.size
    chunks = []
    while 1:
      if length <= 0:
        break
      c = self.f.read(min(self.chunk_size, length))
      if c == "":
        break
      chunks.append(c)
      length -= len(c)
    return "".join(chunks)

  def read_chunk(self, chunk):
    """
    Works like :meth:`read`\ , but data is stored in the writable
    buffer ``chunk`` rather than returned. Reads at most a number of
    bytes equal to the size of ``chunk``\ .

    :type chunk: writable string buffer
    :param chunk: a c-like string buffer, such as the one returned by the
      ``create_string_buffer`` function in the :mod:`ctypes` module
    :rtype: int
    :return: the number of bytes read
    """
    _complain_ifclosed(self.closed)
    return self.f.read_chunk(chunk)
  
  def seek(self, position, whence=os.SEEK_SET):
    """
    Seek to ``position`` in file.

    :type position: int
    :param position: offset in bytes to seek to
    :type whence: int
    :param whence: defaults to ``os.SEEK_SET`` (absolute); other
      values are ``os.SEEK_CUR`` (relative to the current position)
      and ``os.SEEK_END`` (relative to the file's end).
    """
    _complain_ifclosed(self.closed)
    if whence == os.SEEK_CUR:
      position += self.tell()
    elif whence == os.SEEK_END:
      position += self.size
    position = max(0, position)
    self.__reset()
    return self.f.seek(position)
  
  def tell(self):
    """
    Get the current byte offset in the file.

    :rtype: int
    :return: current offset in bytes
    """
    _complain_ifclosed(self.closed)
    return self.f.tell()
  
  def write(self, data):
    """
    Write ``data`` to the file.

    :type data: string
    :param data: the data to be written to the file
    :rtype: int
    :return: the number of bytes written
    """
    _complain_ifclosed(self.closed)
    return self.f.write(data)
  
  def write_chunk(self, chunk):
    """
    Write data from buffer ``chunk`` to the file.

    :type chunk: writable string buffer
    :param chunk: a c-like string buffer, such as the one returned by the
      ``create_string_buffer`` function in the :mod:`ctypes` module
    :rtype: int
    :return: the number of bytes written
    """
    _complain_ifclosed(self.closed)
    return self.f.write_chunk(chunk)

  def flush(self):
    """
    Force any buffered output to be written.
    """
    _complain_ifclosed(self.closed)
    return self.f.flush()


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
  def __init__(self, host, port, user=None, groups=[]):
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
                readline_chunk_size=hdfs_file.DEFAULT_CHUNK_SIZE):
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


def open(hdfs_path, mode="r", buff_size=0, replication=0, blocksize=0,
         readline_chunk_size=hdfs_file.DEFAULT_CHUNK_SIZE, user=None):
  """
  Open a file, returning an :class:`hdfs_file` object.

  ``hdfs_path`` and ``user`` are passed to
  :func:`~pydoop.utils.split_hdfs_path`, while the other args are
  passed to the :class:`hdfs_file` constructor.
  """
  host, port, path = split_hdfs_path(hdfs_path, user)
  fs = hdfs(host, port, user)
  return fs.open_file(path, mode, buff_size, replication, blocksize,
                      readline_chunk_size)
