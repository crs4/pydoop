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

#include <string>

#include <stdint.h>
#include <hadoop/Pipes.hh>
#include "pipes_test_support.hpp"


bp::tuple get_record_from_record_reader(RecordReader* rr) {
  std::string k;
  std::string v;
  bool f = rr->next(k, v);
  bp::str key(k);
  bp::str value(v);
  return bp::make_tuple(f, key, value);
}

float get_progress_from_record_reader(RecordReader* rr) {
  float p = rr->getProgress();
  return p;
}

int get_partition_from_partitioner(Partitioner* pr,
				   std::string k, int n_partitions) {
  int p = pr->partition(k, n_partitions);
  return p;
}


const char* double_a_string(const std::string& a) {
  std::cerr << "read in str " << a << std::endl;
  bp::str ps(a);
  bp::object r = "%s.%s" % bp::make_tuple(ps, ps);
  bp::incref(bp::object(r).ptr());
  const char* p = bp::extract<const char*>(r);
  std::cerr << "p=" << p << std::endl;
  return p;
}

bp::str create_a_string(std::size_t n) {
  std::string s(n, 'c');
  bp::str r(s);
  return r;
}


//++++++++++++++++++++++++++++++//
// Exporting class definitions. //
//++++++++++++++++++++++++++++++//

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
  def("create_a_string", create_a_string);
}
