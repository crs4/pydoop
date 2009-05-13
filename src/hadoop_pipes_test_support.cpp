#include <hadoop/Pipes.hh>

#include <boost/python.hpp>
using namespace boost::python;

#include "hadoop_pipes_test_support.hpp"

#include <iostream>


//+++++++++++++++++++++++++++++++++++++++++
// Exporting class definitions.
//+++++++++++++++++++++++++++++++++++++++++
void export_hadoop_pipes_test_support() 
{
  class_<test_factory>("TestFactory",
		       init<Factory&>())
    .def("createMapper", &test_factory::createMapper,
	 return_value_policy<manage_new_object>())
    .def("createReducer",&test_factory::createReducer,
	 return_value_policy<manage_new_object>())
    ;
  def("wrap_JobConf_object", wrap_JobConf_object,
      return_internal_reference<>());
  def("get_JobConf_object", get_JobConf_object,
      return_value_policy<manage_new_object>());
  def("get_TaskContext_object", get_TaskContext_object,
      return_value_policy<manage_new_object>());
  def("get_MapContext_object", get_MapContext_object,
      return_value_policy<manage_new_object>());
  def("get_ReduceContext_object", get_ReduceContext_object,
      return_value_policy<manage_new_object>());
  def ("double_a_string", double_a_string);
}

