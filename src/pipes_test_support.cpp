#include <hadoop/Pipes.hh>


#include "pipes_test_support.hpp"

#include <string>
#include <iostream>


bp::tuple get_record_from_record_reader(RecordReader* rr){
  std::string k;
  std::string v;
  bool f = rr->next(k, v);
#if 0
  std::cerr << "get_record_from_record_reader: ("<< f 
	    <<", " << k
	    <<", " << v << ")" << std::endl;
#endif
  bp::str key(k);
  bp::str value(v);
  return bp::make_tuple(f, key, value);
}

float get_progress_from_record_reader(RecordReader* rr){
  float p = rr->getProgress();
  return p;
}

int get_partition_from_partitioner(Partitioner* pr, 
				   std::string k, int n_partitions){
  int p = pr->partition(k, n_partitions);
  return p;
}

//+++++++++++++++++++++++++++++++++++++++++
// Exporting class definitions.
//+++++++++++++++++++++++++++++++++++++++++
using namespace boost::python;
void export_pipes_test_support() 
{
  class_<test_factory>("TestFactory",
		       init<Factory*>())
    .def("createRecordReader", &test_factory::createRecordReader,
	 return_value_policy<manage_new_object>())
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
  //
  def("get_record_from_record_reader", get_record_from_record_reader);
  def("get_progress_from_record_reader", get_progress_from_record_reader);
  def("get_partition_from_partitioner", get_partition_from_partitioner);

  def("try_mapper", try_mapper);
  def("try_reducer", try_reducer);
  def("try_factory", try_factory);
  def("try_factory_internal", try_factory_internal);
}

