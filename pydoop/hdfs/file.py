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
pydoop.hdfs.file -- HDFS File Objects
-------------------------------------
"""

import os

import common


def _complain_ifclosed(closed):
  if closed:
    raise ValueError("I/O operation on closed HDFS file object")


class hdfs_file(object):
  """
  Instances of this class represent HDFS file objects.

  Objects from this class should not be instantiated directly.  The
  preferred way to open an HDFS file is with the :func:`open` function;
  alternatively, :meth:`hdfs.open_file` can be used.
  """
  ENDL = os.linesep

  def __init__(self, raw_hdfs_file, fs, name, flags, chunk_size=common.BUFSIZE):
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
    while eol < 0 and not self.EOF:
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


class local_file(file):

  def __init__(self, fs, name, flags):
    if not flags.startswith("r"):
      local_file.__make_parents(fs, name)
    super(local_file, self).__init__(name, flags)
    self.__fs = fs
    self.__name = os.path.abspath(super(local_file, self).name)
    self.__size = os.fstat(super(local_file, self).fileno()).st_size
    self.f = self
    self.chunk_size = 0

  @staticmethod
  def __make_parents(fs, name):
    d = os.path.dirname(name)
    if d:
      try:
        fs.create_directory(d)
      except IOError:
        raise IOError("Cannot open file %s" % name)

  @property
  def fs(self):
    return self.__fs

  @property
  def name(self):
    return self.__name

  @property
  def size(self):
    return self.__size

  def write(self, data):
    super(local_file, self).write(data)
    return len(data)

  def available(self):
    _complain_ifclosed(self.closed)
    return self.size

  def close(self):
    if self.mode == "w":
      self.flush()
      os.fsync(self.fileno())
      self.__size = os.fstat(self.fileno()).st_size
    super(local_file, self).close()

  def pread(self, position, length):
    _complain_ifclosed(self.closed)
    old_pos = self.tell()
    self.seek(position)
    data = self.read(length)
    self.seek(old_pos)
    return data

  def pread_chunk(self, position, chunk):
    _complain_ifclosed(self.closed)
    data = self.pread(position, len(chunk))
    chunk.value = data
    return len(data)

  def read_chunk(self, chunk):
    _complain_ifclosed(self.closed)
    data = self.read(len(chunk))
    chunk.value = data
    return len(data)

  def write_chunk(self, chunk):
    _complain_ifclosed(self.closed)
    return self.write(chunk.value)
