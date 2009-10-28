// BEGIN_COPYRIGHT
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
