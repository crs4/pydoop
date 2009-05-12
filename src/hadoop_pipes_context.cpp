#include <hadoop/Pipes.hh>

#include <boost/python.hpp>
using namespace boost::python;

#include "hadoop_pipes_context.hpp"

#include <iostream>


//+++++++++++++++++++++++++++++++++++++++++
// Exporting class definitions.
//+++++++++++++++++++++++++++++++++++++++++
void export_hadoop_pipes_context() 
{
  //--
  class_<wrap_job_conf, boost::noncopyable>("JobConf")
    .def("hasKey",      pure_virtual(&JobConf::hasKey))
    .def("get",         pure_virtual(&JobConf::get),
	 return_value_policy<copy_const_reference>())	 
    .def("getInt",      pure_virtual(&JobConf::getInt))
    .def("getFloat",    pure_virtual(&JobConf::getFloat))
    .def("getBoolean",  pure_virtual(&JobConf::getBoolean))
    ;
  //--
  class_<TaskContext::Counter>("TaskContext_Counter", init<int>())
    .def("getId", &TaskContext::Counter::getId);
  //--
  class_<wrap_task_context, boost::noncopyable>("TaskContext")
    .def("getJobConf",  pure_virtual(&TaskContext::getJobConf),
	 return_internal_reference<>())
    .def("getInputKey", pure_virtual(&TaskContext::getInputKey),
	 return_value_policy<copy_const_reference>())
    .def("getInputValue", 
	 pure_virtual(&TaskContext::getInputValue),
	 return_value_policy<copy_const_reference>())	 
    .def("emit",     pure_virtual(&TaskContext::emit))
    .def("progress", pure_virtual(&TaskContext::progress))
    .def("setStatus", pure_virtual(&TaskContext::setStatus))
    .def("getCounter", pure_virtual(&TaskContext::getCounter),
	 return_internal_reference<>())
    .def("incrementCounter",
	 pure_virtual(&TaskContext::incrementCounter))
    ;

  //--
  class_<wrap_map_context, bases<TaskContext>, 
    boost::noncopyable>("MapContext")
    .def("getJobConf",  pure_virtual(&MapContext::getJobConf),
	 return_internal_reference<>())
    .def("getInputKey", pure_virtual(&MapContext::getInputKey), 
	 return_value_policy<copy_const_reference>())
    .def("getInputValue", 
	 pure_virtual(&MapContext::getInputValue),
	 return_value_policy<copy_const_reference>())	 
    .def("emit",     pure_virtual(&MapContext::emit))
    .def("progress", pure_virtual(&MapContext::progress))
    .def("setStatus", pure_virtual(&MapContext::setStatus))
    .def("getCounter", pure_virtual(&MapContext::getCounter),
	 return_internal_reference<>())
    .def("incrementCounter",
	 pure_virtual(&MapContext::incrementCounter))
    .def("getInputKeyClass", 
	 pure_virtual(&MapContext::getInputKeyClass), 
	 return_value_policy<copy_const_reference>())	 
    .def("getInputValueClass", 
	 pure_virtual(&MapContext::getInputValueClass),
	 return_value_policy<copy_const_reference>())
    .def("getInputSplit", 
	 pure_virtual(&MapContext::getInputSplit),
	 return_value_policy<copy_const_reference>())
    ;
  //--
  class_<wrap_reduce_context, bases<TaskContext>,
    boost::noncopyable>("ReduceContext")
    .def("getJobConf",  pure_virtual(&ReduceContext::getJobConf),
	 return_internal_reference<>())
    .def("getInputKey", pure_virtual(&ReduceContext::getInputKey), 
	 return_value_policy<copy_const_reference>())
    .def("getInputValue", 
	 pure_virtual(&ReduceContext::getInputValue),
	 return_value_policy<copy_const_reference>())
    .def("emit",     pure_virtual(&ReduceContext::emit))
    .def("progress", pure_virtual(&ReduceContext::progress))
    .def("setStatus", pure_virtual(&ReduceContext::setStatus))
    .def("getCounter", pure_virtual(&ReduceContext::getCounter),
	 return_internal_reference<>())
    .def("incrementCounter",
	 pure_virtual(&ReduceContext::incrementCounter))
    .def("nextValue", 
	 pure_virtual(&ReduceContext::nextValue))
    ;

}

