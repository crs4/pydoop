from pydoop import hdfs

# need to wrap
# hdfsFS
# hdfsFile
# hdfsFileInfo
#
fs = hdfs.connect(host, port, user, groups)
fs = hdfs.connect(host, port)
fs.disconnect()
block_size = fs.get_default_block_size()
capacity   = fs.get_capacity()
used       = fs.get_used()
fs.chown(path, owner, group)
fs.chmod(path, owner, mode)
fs.utime(path, mtime, atime)


yes_no = fs.exists(path)

hdfs.copy(fs_src, src_path, fs_dst, dst_path)
hdfs.move(fs_src, src_path, fs_dst, dst_path)

fs.delete(path)
fs.rename(opath, npath)
dirpath = fs.get_working_directory()
fs.set_working_directory(path)
fs.create_directory(path)
fs.set_replication(path, replication)

file_info_list = fs.list_directory(path)

# deallocate file_info on its destruction
# using hdfsFreeFileInfo()
file_info = fs.get_path_info(path)

hosts_blocks_pairs_list = fs.get_hosts(path,
                                       start, length)









#OpenFile
f = fs.open(path, flags, buff_size,
            replication, block_size)






res = f.seek(offset)
offset = f.tell()

a = np.zeros(buf_size)
n_bytes_read = f.read(a)
n_bytes_read = f.pread(a)
bytes = f.read(buf_size)
bytes = f.pread(offset, buf_size)


a = np.ones(buf_size)
n_bytes_read = f.write(a)

n_bytes_available = f.available()
f.close()



