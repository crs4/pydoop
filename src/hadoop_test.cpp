#include <hadoop/Pipes.hh>
#include <boost/python.hpp>

#include "hadoop_test.hpp"

using namespace boost::python;


void export_hadoop_test()
{
  class_<test_factory>("TestFactory",
		       init<Factory&>())
    .def("createMapper", &test_factory::createMapper,
	 return_value_policy<manage_new_object>())
    .def("createReducer",&test_factory::createReducer,
	 return_value_policy<manage_new_object>())
    ;
}
