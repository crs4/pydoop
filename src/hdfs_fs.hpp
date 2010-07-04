// BEGIN_COPYRIGHT
// END_COPYRIGHT
#ifndef HADOOP_HDFS_FS_HPP
#define HADOOP_HDFS_FS_HPP

#include <string>

#include "hdfs_common.hpp"
#include "hdfs_file.hpp"
#include <boost/python.hpp>

namespace bp = boost::python;


struct wrap_hdfs_fs {
  std::string host_;
  int port_;
  std::string user_;
  hdfsFS fs_;

  wrap_hdfs_fs(std::string host, int port, std::string user, bp::list groups):
    host_(host), port_(port), user_(user) {
    int ng = len(groups);
    const char* host_str = (host_.size() == 0) ? NULL : host_.c_str();
    const char* user_str = (user_.size() == 0) ? NULL : user_.c_str();
    const char **groups_str = NULL;
    if (ng > 0) {
      groups_str = (const char**)malloc(sizeof(char*) * ng);
      for (int k = 0; k < ng; ++k) {
	char* s = bp::extract<char*>(groups[k]);
	groups_str[k] = s;
      }
    }
    hdfsFS fs = hdfsConnectAsUser(host_str, port, user_str, groups_str, ng);
    free(groups_str);
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

  void chown(const std::string& path,
	     const std::string& owner, const std::string& group);

  void chmod(const std::string& path, unsigned short mode);

  void utime(const std::string& path, long mtime, long atime);

  wrap_hdfs_file* open_file(std::string path, int flags, 
			    int buffer_size, int replication, 
			    int blocksize);
};

#endif // HADOOP_HDFS_FS_HPP
