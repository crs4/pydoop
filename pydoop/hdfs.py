# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
This module provides access to HDFS from Python applications.
"""

import os, glob


DEFAULT_HADOOP_HOME = "/opt/hadoop"  # should only be useful for local use
HADOOP_HOME = os.getenv("HADOOP_HOME", DEFAULT_HADOOP_HOME)
HADOOP_CONF_DIR = os.getenv("HADOOP_CONF_DIR",
                            os.path.join(HADOOP_HOME, "conf"))

jars = glob.glob(os.path.join(HADOOP_HOME, "lib/*.jar"))
jars.extend(glob.glob(os.path.join(HADOOP_HOME, "hadoop*.jar")))
jars.append(HADOOP_CONF_DIR)

CLASSPATH = os.environ.setdefault("CLASSPATH", "")
CLASSPATH = "%s:%s" % (":".join(jars), CLASSPATH)
os.environ["CLASSPATH"] = CLASSPATH


from pydoop_hdfs import hdfs_fs


class hdfs_file(object):
  """
  A wrapper for raw hdfs files. Currently it adds the readline method.
  """
  
  DEFAULT_CHUNK_SIZE = 16384
  ENDL = os.linesep

  def __init__(self, raw_hdfs_file, chunk_size=DEFAULT_CHUNK_SIZE):
    """
    C{hdfs_file} should not be instantiated directly. To get an hdfs
    file object, call L{hdfs.open_file}.
    """
    if not chunk_size > 0:
      raise ValueError("chunk size must be positive")
    self.f = raw_hdfs_file
    self.chunk_size = chunk_size
    self.__reset()

  def __reset(self):
    self.buffer_list = []
    self.chunk = ""
    self.EOF = False
    self.p = 0

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
      if i == 20: break
      if self.p < len(self.chunk):
        self.buffer_list.append(self.chunk[self.p:])
      self.__read_chunk()
      eol = self.chunk.find(self.ENDL, self.p)
    return eol if eol > -1 else len(self.chunk)

  def readline(self):
    """
    Reads and returns a line of text.
    """
    eol = self.__read_chunks_until_nl()
    line = "".join(self.buffer_list) + self.chunk[self.p:eol+1]
    self.buffer_list = []
    self.p = eol+1
    return line
     
  def available(self):
    """
    Number of bytes that can be read from this input stream without blocking.
    """
    return self.f.available()
  
  def close(self):
    """
    Close the file.
    """
    return self.f.close()
  
  def pread(self, position, length):
    """
    Read C{length} bytes of data from the file, starting from C{position}.
    """
    return self.f.pread(position, length)
  
  def pread_chunk(self, position, chunk):
    """
    Works like L{pread}, but data is stored in the writable buffer C{chunk}.

    @param position: starting position from which to read.
    @param chunk: a writable buffer (e.g., C{ctypes.create_string_buffer})

    @return: the number of bytes read; -1 on error
    """    
    return self.f.pread_chunk(position, chunk)
  
  def read(self, length):
    """
    Read C{length} bytes from the file.
    """
    return self.f.read(length)
 
  def read_chunk(self, chunk):
    """
    Works like L{read}, but data is stored in the writable buffer C{chunk}.

    @param chunk: a writable buffer (e.g., C{ctypes.create_string_buffer})

    @return: the number of bytes read; -1 on error
    """    
    
    return self.f.read_chunk(chunk)
  
  def seek(self, position):
    """
    Seek to given offset in file.
    """
    self.__reset()
    return self.f.seek(position)
  
  def tell(self):
    """
    Get the current byte offset in the file.
    """
    return self.f.tell()
  
  def write(self, data):
    """
    Write data to the file.
    """
    return self.f.write(data)
  
  def write_chunk(self, chunk):
    """
    Write data from buffer C{chunk} to the file.
    """
    return self.f.write_chunk(chunk)
    

class hdfs(hdfs_fs):
  """
  Represents a connection to an HDFS file system.
  """
  def __init__(self, host, port):
    """
    @param host: hostname or IP address of the HDFS NameNode. Set to
      an empty string (and port to 0) to connect to the local file
      system; Set to 'default' (and port to 0) to connect to the
      'configured' file system.
    @param port: the port on which the NameNode is listening.
    """
    super(hdfs, self).__init__(host, port)

  def open_file(self, path,
                flags=os.O_RDONLY,
                buff_size=0,
                replication=0,
                blocksize=0,
                readline_chunk_size=hdfs_file.DEFAULT_CHUNK_SIZE):
    """
    Open an hdfs file.

    Pass 0 as buff_size, replication or blocksize if you want to use
    the default configured values.

    @param path: the full path to the file.
    @param flags: opening flags - supported flags are os.O_RDONLY, os.O_WRONLY
    @param buff_size: read/write buffer size.
    @param replication: HDFS block replication.
    @param blocksize: HDFS block size.
    @param readline_chunk_size: the amount of bytes that L{hdfs_file.readline}
      will use as buffer.

    @return: handle to the open file
    """
    return hdfs_file(super(hdfs, self).open_file(path, flags, buff_size,
                                                 replication, blocksize),
                     readline_chunk_size)
