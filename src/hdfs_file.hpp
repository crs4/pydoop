#ifndef HADOOP_HDFS_FILE_HPP
#define HADOOP_HDFS_FILE_HPP

#include <string>
#include <iostream>

#include "hdfs_common.hpp"

#include <boost/python.hpp>
namespace bp = boost::python;

//+++++++++++++++++++++++++++++++++++++++++++++++++++++//
//                 hdfs_file                           //
//+++++++++++++++++++++++++++++++++++++++++++++++++++++//

// forward declaration
struct wrap_hdfs_fs;

struct wrap_hdfs_file_info {
  // FIXME to be filled.
};

struct wrap_hdfs_file {
  const std::string filename_;
  wrap_hdfs_fs      *fs_;
  hdfsFile          file_;
  bool              is_open_;

  wrap_hdfs_file(std::string fn, wrap_hdfs_fs *fs, hdfsFile f) 
    : filename_(fn), fs_(fs), file_(f), is_open_(true) {
    std::cerr << "created file " << filename_ << std::endl;
  }
  
  ~wrap_hdfs_file(){ 
    close(); 
  }

  void close(){
    if (is_open_) {
    _close_helper();    
    }
  }
    
  //-----------------------------------------------
  void seek(tOffset desidered_pos); 
  //-----------------------------------------------
  int tell();
  //-----------------------------------------------
  std::string read(tSize length);
  //-----------------------------------------------
  std::string pread(tOffset position, tSize length);
  //-----------------------------------------------
  tSize write(std::string buffer);

  //-----------------------------------------------
#if 0
  //-----------------------------------------------
  int read_chunk();
  //-----------------------------------------------
  int pread_chunk();
#endif

  //-----------------------------------------------
  void _close_helper();
};


#endif // HADOOP_HDFS_FILE_HPP
