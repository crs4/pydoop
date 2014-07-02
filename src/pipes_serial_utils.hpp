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

#ifndef HADOOP_PIPES_SERIAL_UTILS_HPP
#define HADOOP_PIPES_SERIAL_UTILS_HPP

#include <string>
#include "hadoop/SerialUtils.hh"

#ifdef __APPLE__
#include "mac_support.hpp"
#endif

#include <boost/python.hpp>

namespace bp = boost::python;
namespace hu = HadoopUtils;


class _StringOutStream: public hu::OutStream {
protected:
  std::ostringstream _os;
public:
  _StringOutStream();
  void write(const void *buff, std::size_t len);
  void flush();
  std::string str();
};

class _StringInStream: public hu::InStream {
protected:
  std::istringstream _is; 
public:
  _StringInStream(const std::string& s);
  void read(void *buff, std::size_t len);
  uint16_t readUShort();
  uint64_t readLong();
  void seekg(std::size_t offset);
  std::size_t tellg();
};

std::string pipes_serialize_int(long t);
std::string pipes_serialize_float(float t);
std::string pipes_serialize_string(std::string t);
bp::tuple pipes_deserialize_int(const std::string& s, std::size_t offset);
bp::tuple pipes_deserialize_float(const std::string& s, std::size_t offset);
bp::tuple pipes_deserialize_string(const std::string& s, std::size_t offset);

#endif // HADOOP_PIPES_SERIAL_UTILS_HPP
