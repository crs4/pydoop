#ifndef HADOOP_HDFS_FS_HPP
#define HADOOP_HDFS_FS_HPP

#include <string>
#include <iostream>

#include "hdfs_common.hpp"
#include "hdfs_file.hpp"

#include <boost/python.hpp>
namespace bp = boost::python;


struct wrap_hdfs_fs {
  std::string host_;
  int         port_;
  hdfsFS      fs_;

  wrap_hdfs_fs(std::string host, int port) 
    : host_(host), port_(port) {
    //-
    const char* host_str = (host.size() == 0)? NULL : host.c_str();
    hdfsFS fs = hdfsConnect(host_str, port);
    if (fs == NULL) {
      throw hdfs_exception("Cannot connect to " + host);
    }
    fs_ = fs;
  }

  wrap_hdfs_file* open_file(std::string path, int flags, 
			    int buffer_size, int replication, 
			    int blocksize);
};




#endif // HADOOP_HDFS_FS_HPP
