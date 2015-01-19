# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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
Abstract low-level HDFS interface.
"""

from abc import ABCMeta, abstractmethod

from pydoop.hdfs.common import BUFSIZE


class CoreHdfsFs(object):
    """
    Abstract filesystem interface.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def open_file(self, path, flags=0, buff_size=0, replication=1, blocksize=0,
                  readline_chunk_size=16384):
        pass

    #@property
    def capacity(self):
        return self.get_capacity()

    @abstractmethod
    def get_capacity(self):
        pass

    @abstractmethod
    def copy(self, from_path, to_hdfs, to_path):
        pass

    @abstractmethod
    def create_directory(self, path):
        pass

    #@property
    def default_block_size(self):
        return self.get_default_block_size()

    @abstractmethod
    def get_default_block_size(self):
        pass

    @abstractmethod
    def delete(self, path, recursive=True):
        pass

    @abstractmethod
    def exists(self, path):
        pass

    @abstractmethod
    def get_hosts(self, path, start, length):
        pass

    @abstractmethod
    def list_directory(self, path):
        pass

    @property
    def path_info(self, path):
        return self.get_path_info(path)

    @abstractmethod
    def get_path_info(self, path):
        pass

    @abstractmethod
    def move(self, from_path, to_hdfs, to_path):
        pass

    @abstractmethod
    def rename(self, from_path, to_path):
        pass

    @abstractmethod
    def set_replication(self, path, replication):
        pass

    @abstractmethod
    def set_working_directory(self, path):
        pass

    #@property
    def used(self):
        return self.get_used()

    @abstractmethod
    def get_used(self):
        pass

    #@property
    def working_directory(self):
        return self.get_working_directory()

    @abstractmethod
    def get_working_directory(self):
        pass

    @abstractmethod
    def chmod(self, path, mode):
        pass

    @abstractmethod
    def chown(self, path, user, group):
        pass

    @abstractmethod
    def utime(self, path, mtime, atime):
        pass


class CoreHdfsFile(object):
    """
    Abstract file object interface.
    """

    __metaclass__ = ABCMeta

    @property
    def mode(self):
        return self.get_mode()

    @abstractmethod
    def get_mode(self):
        pass

    @abstractmethod
    def available(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def read(self, size=BUFSIZE):
        pass

    @abstractmethod
    def pread(self, position, length):
        pass

    @abstractmethod
    def read_chunk(self, chunk):
        pass

    @abstractmethod
    def pread_chunk(self, position, chunk):
        pass

    @abstractmethod
    def seek(self, pos):
        pass

    @abstractmethod
    def tell(self):
        pass

    @abstractmethod
    def write(self, data):
        pass

    @abstractmethod
    def write_chunk(self, chunk):
        pass

    @abstractmethod
    def flush(self):
        pass
