// BEGIN_COPYRIGHT
// 
// Copyright 2009-2013 CRS4.
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
#include <iostream>


namespace bp = boost::python;

struct payload {
  int v;
  payload(int x) : v(x){}
  int get() {
    return v;
  }
  void set(int x){
    v = x;
  }
  virtual ~payload() {
    std::cerr << "payload::Destroying payload(" << v << ")\n" << std::endl;
  }
};

void payload_user(bp::object payload_maker){
  typedef std::auto_ptr<payload> auto_payload;
  std::cerr << "payload_user: -- 0 --" << std::endl;
  bp::object pl = payload_maker(20);
  std::cerr << "payload_user: -- 1 --" << std::endl;
  payload* p = bp::extract<payload*>(pl);
  std::cerr << "payload_user: extract -> " << p << std::endl;
  std::cerr << "payload_user: p->get() " << p->get() << std::endl;
  auto_payload ap = bp::extract<auto_payload>(pl);
  p = ap.get();
  std::cerr << "payload_user: ap.get() -> " << p << std::endl;
  ap.release();
  delete p;
  std::cerr << "payload_user: -- 2 --" << std::endl;
}


BOOST_PYTHON_MODULE(iref)
{
  using namespace boost::python;

  class_<payload, std::auto_ptr<payload> >("payload", init<int>())
    .def("get", &payload::get)
    .def("set", &payload::set)
    ;
  def("payload_user", payload_user);
}

