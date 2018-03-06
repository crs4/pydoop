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
pydoop.hdfs.file -- HDFS File Objects
-------------------------------------
"""

import os
import io
import codecs

from pydoop.hdfs import common


def _complain_ifclosed(closed):
    if closed:
        raise ValueError("I/O operation on closed HDFS file object")


class FileIO(object):
    """
    Instances of this class represent HDFS file objects.

    Objects from this class should not be instantiated directly.  To
    open an HDFS file, use :meth:`~.fs.hdfs.open_file`, or the
    top-level ``open`` function in the hdfs package.
    """
    ENCODING = "utf-8"
    ERRORS = "strict"

    def __init__(self, raw_hdfs_file, fs, mode, encoding=None, errors=None):
        self.mode = mode
        self.base_mode, is_text = common.parse_mode(self.mode)
        self.buff_size = raw_hdfs_file.buff_size
        if self.buff_size <= 0:
            self.buff_size = common.BUFSIZE
        if is_text:
            self.__encoding = encoding or self.__class__.ENCODING
            self.__errors = errors or self.__class__.ERRORS
            try:
                codecs.lookup(self.__encoding)
                codecs.lookup_error(self.__errors)
            except LookupError as e:
                raise ValueError(e)
        else:
            if encoding:
                raise ValueError(
                    "binary mode doesn't take an encoding argument")
            if errors:
                raise ValueError("binary mode doesn't take an errors argument")
            self.__encoding = self.__errors = None
        cls = io.BufferedReader if self.base_mode == "r" else io.BufferedWriter
        self.f = cls(raw_hdfs_file, buffer_size=self.buff_size)
        self.__fs = fs
        info = fs.get_path_info(self.f.raw.name)
        self.__name = info["name"]
        self.__size = info["size"]
        self.closed = False

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

    def writable(self):
        return self.f.raw.writable()

    def readline(self):
        """
        Read and return a line of text.

        :rtype: str
        :return: the next line of text in the file, including the
          newline character
        """
        _complain_ifclosed(self.closed)
        line = self.f.readline()
        if self.__encoding:
            return line.decode(self.__encoding, self.__errors)
        else:
            return line

    def next(self):
        """
        Return the next input line, or raise :class:`StopIteration`
        when EOF is hit.
        """
        return self.__next__()

    def __next__(self):
        """
        Return the next input line, or raise :class:`StopIteration`
        when EOF is hit.
        """
        _complain_ifclosed(self.closed)
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def __iter__(self):
        return self

    def available(self):
        """
        Number of bytes that can be read from this input stream without
        blocking.

        :rtype: int
        :return: available bytes
        """
        _complain_ifclosed(self.closed)
        return self.f.raw.available()

    def close(self):
        """
        Close the file.
        """
        if not self.closed:
            self.closed = True
            retval = self.f.close()
            if self.base_mode != "r":
                self.__size = self.fs.get_path_info(self.name)["size"]
            return retval

    def pread(self, position, length):
        r"""
        Read ``length`` bytes of data from the file, starting from
        ``position``\ .

        :type position: int
        :param position: position from which to read
        :type length: int
        :param length: the number of bytes to read
        :rtype: string
        :return: the chunk of data read from the file
        """
        _complain_ifclosed(self.closed)
        if position > self.size:
            raise IOError("position cannot be past EOF")
        if length < 0:
            length = self.size - position
        data = self.f.raw.pread(position, length)
        if self.__encoding:
            return data.decode(self.__encoding, self.__errors)
        else:
            return data

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
            c = self.f.read(min(self.buff_size, length))
            if c == b"":
                break
            chunks.append(c)
            length -= len(c)
        data = b"".join(chunks)
        if self.__encoding:
            return data.decode(self.__encoding, self.__errors)
        else:
            return data

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
        return self.f.seek(position, whence)

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

        :type data: bytes
        :param data: the data to be written to the file
        :rtype: int
        :return: the number of bytes written
        """
        _complain_ifclosed(self.closed)
        if self.__encoding:
            self.f.write(data.encode(self.__encoding, self.__errors))
            return len(data)
        else:
            return self.f.write(data)

    def flush(self):
        """
        Force any buffered output to be written.
        """
        _complain_ifclosed(self.closed)
        return self.f.flush()


class hdfs_file(FileIO):

    def pread_chunk(self, position, chunk):
        r"""
        Works like :meth:`pread`\ , but data is stored in the writable
        buffer ``chunk`` rather than returned. Reads at most a number of
        bytes equal to the size of ``chunk``\ .

        :type position: int
        :param position: position from which to read
        :type chunk: buffer
        :param chunk: a writable object that supports the buffer protocol
        :rtype: int
        :return: the number of bytes read
        """
        _complain_ifclosed(self.closed)
        if position > self.size:
            raise IOError("position cannot be past EOF")
        return self.f.raw.pread_chunk(position, chunk)

    def read_chunk(self, chunk):
        r"""
        Works like :meth:`read`\ , but data is stored in the writable
        buffer ``chunk`` rather than returned. Reads at most a number of
        bytes equal to the size of ``chunk``\ .

        :type chunk: buffer
        :param chunk: a writable object that supports the buffer protocol
        :rtype: int
        :return: the number of bytes read
        """
        _complain_ifclosed(self.closed)
        return self.f.readinto(chunk)


class local_file(io.FileIO):
    """\
    Support class to handle local files.

    Objects from this class should not be instantiated directly, but
    rather obtained through the top-level ``open`` function in the
    hdfs package.
    """
    def __init__(self, fs, name, mode):
        if not mode.startswith("r"):
            local_file.__make_parents(fs, name)
        super(local_file, self).__init__(name, mode)
        name = os.path.abspath(name)
        self.__fs = fs
        self.__size = os.fstat(super(local_file, self).fileno()).st_size
        self.f = self
        self.buff_size = io.DEFAULT_BUFFER_SIZE

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
    def size(self):
        return self.__size

    def available(self):
        _complain_ifclosed(self.closed)
        return self.size

    def close(self):
        if self.writable():
            self.flush()
            os.fsync(self.fileno())
            self.__size = os.fstat(self.fileno()).st_size
        super(local_file, self).close()

    def seek(self, position, whence=os.SEEK_SET):
        if position > self.__size:
            raise IOError("position cannot be past EOF")
        return super(local_file, self).seek(position, whence)

    def __seek_and_read(self, position, length=None, buf=None):
        assert (length is None) != (buf is None)
        _complain_ifclosed(self.closed)
        old_pos = self.tell()
        self.seek(position)
        if buf is not None:
            ret = self.readinto(buf)
        else:
            if length < 0:
                length = self.size - position
            ret = self.read(length)
        self.seek(old_pos)
        return ret

    def pread(self, position, length):
        return self.__seek_and_read(position, length=length)

    def pread_chunk(self, position, chunk):
        return self.__seek_and_read(position, buf=chunk)

    def read_chunk(self, chunk):
        _complain_ifclosed(self.closed)
        return self.readinto(chunk)


class TextIOWrapper(io.TextIOWrapper):

    def __getattr__(self, name):
        # there is no readinto method in text mode (strings are immutable)
        if name.endswith("_chunk"):
            raise AttributeError("%r object has no attribute %r" % (
                self.__class__.__name__, name
            ))
        a = getattr(self.buffer.raw, name)
        if name == "mode":
            a = "%st" % self.buffer.raw.mode[0]
        return a

    def pread(self, position, length):
        data = self.buffer.raw.pread(position, length)
        return data.decode(self.encoding, self.errors)
