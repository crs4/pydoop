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

#include <boost/python.hpp>
#include "exceptions.hpp"


void pipes_exception_translator(pipes_exception const& x) {
  std::string w(x.what());
  std::string msg = "pydoop_exception.pipes: " + w;
  PyErr_SetString(PyExc_UserWarning, msg.c_str());
}

void pydoop_exception_translator(pydoop_exception const& x) {
  std::string w(x.what());
  std::string msg = "pydoop_exception: " + w;
  PyErr_SetString(PyExc_UserWarning, msg.c_str());
}

void raise_pydoop_exception(std::string s) {
  throw pydoop_exception(s);
}

void raise_pipes_exception(std::string s) {
  throw pipes_exception(s);
}


using namespace boost::python;

void export_exceptions() {
  def("raise_pydoop_exception", raise_pydoop_exception);
  def("raise_pipes_exception",  raise_pipes_exception);
  register_exception_translator<pydoop_exception>(pydoop_exception_translator);
  register_exception_translator<pipes_exception>(pipes_exception_translator);
}
