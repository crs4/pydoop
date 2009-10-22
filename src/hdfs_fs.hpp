#ifndef HADOOP_HDFS_FS_HPP
#define HADOOP_HDFS_FS_HPP

#include <string>

#include "hdfs_common.hpp"
#include "hdfs_file.hpp"
#include <boost/python.hpp>

namespace bp = boost::python;


struct wrap_hdfs_fs {
  std::string host_;
  int         port_;
  hdfsFS      fs_;

  wrap_hdfs_fs(std::string host, int port): host_(host), port_(port) {
    const char* host_str = (host_.size() == 0)? NULL : host_.c_str();
    hdfsFS fs = hdfsConnect(host_str, port);
    if (fs == NULL) {
      throw hdfs_exception("Cannot connect to " + host);
    }
    fs_ = fs;
  }
  void disconnect();
  tOffset get_default_block_size();
  tOffset get_capacity();
  tOffset get_used();

  bp::list list_directory(std::string path);
  bp::dict get_path_info(std::string path);
  bp::list get_hosts(std::string path, tOffset start, tOffset length);

  bool exists(const std::string& path);
  void unlink(const std::string& path);
  void copy(const std::string& src_path, 
	    wrap_hdfs_fs& dst_fs, const std::string& dst_path);
  void move(const std::string& src_path, 
	    wrap_hdfs_fs& dst_fs, const std::string& dst_path);
  void rename(const std::string& old_path, const std::string& new_path);

  std::string get_working_directory();
  void set_working_directory(const std::string& path);
  void create_directory(const std::string& path);

  void set_replication(const std::string& path, int replication);

  wrap_hdfs_file* open_file(std::string path, int flags, 
			    int buffer_size, int replication, 
			    int blocksize);
};

#endif // HADOOP_HDFS_FS_HPP
