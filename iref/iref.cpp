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

