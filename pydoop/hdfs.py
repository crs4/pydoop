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
    if not chunk_size > 0:
      raise ValueError("chunk size must be positive")
    self.f = raw_hdfs_file
    self.chunk_size = chunk_size
    self.buffer_list = []
    self.chunk = ""
    self.EOF = False
    self.p = 0

  def __read_chunk(self):
    self.chunk = self.f.read(self.chunk_size)
    self.p = 0
    if len(self.chunk) < self.chunk_size:
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
    eol = self.__read_chunks_until_nl()
    line = "".join(self.buffer_list) + self.chunk[self.p:eol+1]
    self.buffer_list = []
    self.p = eol+1
    return line
     
  def available(self):
    return self.f.available()
  
  def close(self):
    return self.f.close()
  
  def pread(self, position, length):
    return self.f.pread(position, length)
  
  def pread_chunk(self, position, chunk):
    return self.f.pread_chunk(position, chunk)
  
  def read(self, length):
    return self.f.read(length)
 
  def read_chunk(self, chunk):
    return self.f.read_chunk(chunk)
  
  def seek(self, position):
    return self.f.seek(position)
  
  def tell(self):
    return self.f.tell()
  
  def write(self, data):
    return self.f.write(data)
  
  def write_chunk(self, chunk):
    return self.f.write_chunk(chunk)
    

class hdfs(hdfs_fs):

  def __init__(self, host, port):
    super(hdfs, self).__init__(host, port)

  def open_file(self, path,
                flags=os.O_RDONLY,
                buff_size=0,
                replication=0,
                blocksize=0,
                readline_chunk_size=hdfs_file.DEFAULT_CHUNK_SIZE):
    return hdfs_file(super(hdfs, self).open_file(path, flags, buff_size,
                                                 replication, blocksize),
                     readline_chunk_size)
