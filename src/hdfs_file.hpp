// BEGIN_COPYRIGHT
// 
// Copyright 2009-2014 CRS4.
// 
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
// 
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
