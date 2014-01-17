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

#ifndef HADOOP_EXCEPTIONS_HPP
#define HADOOP_EXCEPTIONS_HPP

#include <boost/python.hpp>

namespace bp = boost::python;


class pydoop_exception: public std::exception {
private:
  const std::string msg_;
public:
  pydoop_exception(std::string msg) : msg_(msg) {}
  virtual const char* what() const throw() {
    return msg_.c_str();
  }
  virtual ~pydoop_exception() throw() {}
};


class pipes_exception: public pydoop_exception {
public:
  pipes_exception(std::string msg) : pydoop_exception(msg) {}
  bp::tuple args(void) {
    return bp::make_tuple(what());
  }
};

#endif // HADOOP_EXCEPTIONS_HPP
