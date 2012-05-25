// BEGIN_COPYRIGHT
// 
// Copyright 2012 CRS4.
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

#include <hadoop/StringUtils.hh>
#include <hadoop/SerialUtils.hh>
#include <boost/python.hpp>
#include <string>

#include "utils.hpp"

namespace bp = boost::python;
namespace hu = HadoopUtils;


std::string deserialize_string(const std::string& s) {
  std::string ds;
  hu::StringInStream stream(s);
  hu::deserializeString(ds, stream);
  return ds;
}

std::string serialize_string(const std::string& s) {
  std::string ss;
  hu::StringOutStream stream(s);
  hu::deserializeString(ds, stream);
  return ds;
}


//++++++++++++++++++++++++++++++//
// Exporting class definitions. //
//++++++++++++++++++++++++++++++//

using namespace boost::python;

void export_utils() {
  def("deserialize_string", deserialize_string);
}
