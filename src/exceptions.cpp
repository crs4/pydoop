
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

void raise_pydoop_exception(std::string s){
  throw pydoop_exception(s);
}

void raise_pipes_exception(std::string s){
  throw pipes_exception(s);
}

void export_exceptions() 
{
  using namespace boost::python;
#if 0
  //--
  class_<pydoop_exception>("pydoop_exception", init<std::string>())
    .def("args",  &pipes_exception::args)
    .def("message", &pipes_exception::what)
    ;

  //--
  class_<pipes_exception>("pipes_exception", init<std::string>())
    .def("args",  &pipes_exception::args)
    .def("message", &pipes_exception::what)
    ;
#endif
  def("raise_pydoop_exception", raise_pydoop_exception);
  def("raise_pipes_exception",  raise_pipes_exception);

  register_exception_translator<pydoop_exception>(pydoop_exception_translator);
  register_exception_translator<pipes_exception>(pipes_exception_translator);
}





