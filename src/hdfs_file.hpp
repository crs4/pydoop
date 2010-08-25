// BEGIN_COPYRIGHT
// END_COPYRIGHT

#ifndef HADOOP_HDFS_FILE_HPP
#define HADOOP_HDFS_FILE_HPP

#include <string>

#include "hdfs_common.hpp"
#include <boost/python.hpp>

namespace bp = boost::python;


//+++++++++++++++++++++++++++++++++++++++++++++++++++++//
//                 hdfs_file                           //
//+++++++++++++++++++++++++++++++++++++++++++++++++++++//

// forward declaration
struct wrap_hdfs_fs;


struct wrap_hdfs_file {
  const std::string filename_;
  wrap_hdfs_fs      *fs_;
  hdfsFile          file_;
  bool              is_open_;

  wrap_hdfs_file(std::string fn, wrap_hdfs_fs *fs, hdfsFile f) 
    : filename_(fn), fs_(fs), file_(f), is_open_(true) {
  }
  
  ~wrap_hdfs_file(){ close(); }
  void close(){ if (is_open_) {_close_helper();} }
  void seek(tOffset desidered_pos); 
  tOffset tell();
  std::string read(tSize length);
  std::string pread(tOffset position, tSize length);
  tSize write(const std::string& buffer);
  tSize read_chunk(bp::object buffer);
  tSize pread_chunk(tOffset position, bp::object buffer);
  tSize write_chunk(bp::object buffer);
  int available();
  void flush();

  void _close_helper();
};

#endif // HADOOP_HDFS_FILE_HPP
