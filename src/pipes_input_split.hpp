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

// In Hadoop <= 0.20.2, FileSplit has the following format:
//     16 bit filename byte length
//     filename in bytes
//     64 bit offset
//     64 bit length
// Starting from release 0.21.0, the first field is a variable length
// compressed long. For details, see:
//     mapred/src/java/org/apache/hadoop/mapreduce/lib/input/FileSplit.Java
//     --> readFields
//     common/src/java/org/apache/hadoop/io/Text.java
//     --> readString
//     common/src/java/org/apache/hadoop/io/WritableUtils.java
//     --> readVInt  
// in Hadoop's source code.


#ifndef HADOOP_PIPES_INPUT_SPLIT_HPP
#define HADOOP_PIPES_INPUT_SPLIT_HPP

#include <stdint.h>
#include <string>
#include "pipes_serial_utils.hpp"

namespace bp = boost::python;


struct wrap_input_split {
protected:
  std::string filename_;
  bp::long_ offset_, length_;
public:
  wrap_input_split(const std::string& raw_input_split) {
    _StringInStream is(raw_input_split);
#ifdef VINT_ISPLIT_FILENAME
    hu::deserializeString(filename_, is);
#else
    uint16_t fn_len = is.readUShort();
    char *buf = new char[fn_len];
    is.read(buf, fn_len);
    filename_.assign(buf, fn_len);
    delete[] buf;
#endif
    offset_ = bp::long_(is.readLong());
    length_ = bp::long_(is.readLong());
  }
  std::string filename();
  bp::long_ offset();
  bp::long_ length();
};

#endif // HADOOP_PIPES_INPUT_SPLIT_HPP
