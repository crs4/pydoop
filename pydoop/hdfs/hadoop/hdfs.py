__author__ = 'kikkomep'

from abc import abstractmethod

from pydoop.hdfs.hadoop import get_implementation_instance


class FileSystem(object):

    """

    """
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def open_file(self, path, flags=0, buff_size=0, replication=1, blocksize=0, readline_chunk_size=16384):
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


class File(object):

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
    def read(self, chunk_size):
        pass

    @abstractmethod
    def pread(self, position, length):
        pass

    @abstractmethod
    def read_check(self, chunk):
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


if __name__ == '__main__':
    fs = get_implementation_instance("FileSystem", host="default")
    print "the working directory: %s " % fs.get_working_directory()
    #fs.create_directory("pippo")


    #fs.copy("LICENSE", ".", "L1")
    fs.chown("foo", "kikkomep", "users")
    # fs.chmod("input", 0000)
    #print fs.get_path_info("/user/kikkomep/output")
    #print "%s" % fs.exists("pippo")
    #fs.copy("ChangeLog", ".")
    #print "%s" % get_hadoop_version()
    # for d in fs.list_directory("."):
    #     print d
    #print fs.default_block_size
    #fs.delete("./L1")
    #fs.move("L1", fs.path_info)
    #fs.move("L1", "hdfs://localhost:8020", "XXX")

    # f = fs.open_file("input", "r")
    # l = f.readline()
    # print l
    #b = create_string_buffer(512)
    #data = f.read(b)

    #for i in data:
        #print "%s" % str.decode(str(i), 'utf-8')

    #print b
    #print "%s" % fs.get_working_directory()











