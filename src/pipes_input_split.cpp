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

#include "pipes_input_split.hpp"

namespace bp = boost::python;


std::string wrap_input_split::filename() {
  return filename_;
}

bp::long_ wrap_input_split::offset() {
  return offset_;
}

bp::long_ wrap_input_split::length() {
  return length_;
}


//++++++++++++++++++++++++++++++//
// Exporting class definitions. //
//++++++++++++++++++++++++++++++//

using namespace boost::python;

void export_input_split() {
  class_<wrap_input_split, boost::noncopyable>("input_split", init<std::string>())
    .add_property("filename", &wrap_input_split::filename)
    .add_property("offset", &wrap_input_split::offset)
    .add_property("length", &wrap_input_split::length)
    ;
}
