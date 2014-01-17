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

#include <stdint.h>
#include <hadoop/Pipes.hh>
#include "pipes.hpp"
#include <boost/python.hpp>


//++++++++++++++++++++++++++++++//
// Exporting class definitions. //
//++++++++++++++++++++++++++++++//

using namespace boost::python;

void export_pipes() {

  class_<wrap_mapper, std::auto_ptr<wrap_mapper>, boost::noncopyable>("Mapper", "Basic wrapping of mapper class")
    .def("map", pure_virtual(&hp::Mapper::map))
    .def("close", pure_virtual(&hp::Mapper::close))
    ;

  class_<wrap_reducer, std::auto_ptr<wrap_reducer>, boost::noncopyable>("Reducer", "Basic wrapping of reducer class")
    .def("reduce", pure_virtual(&hp::Reducer::reduce))
    .def("close", pure_virtual(&hp::Reducer::close))
    ;

  class_<wrap_partitioner, std::auto_ptr<wrap_partitioner>, boost::noncopyable>("Partitioner", "Basic wrapping of Partitioner class")
    .def("partition", pure_virtual(&hp::Partitioner::partition))
    ;

  class_<wrap_record_reader, std::auto_ptr<wrap_record_reader>, boost::noncopyable>("RecordReader", "Basic wrapping of RecordReader class")
    // We disable python view of next, since it is unclear how its
    // call by ref to implement a side-effect should be interpreted.
    //.def("next",pure_virtual(&hp::RecordReader::next))
    .def("getProgress", pure_virtual(&hp::RecordReader::getProgress))
    .def("close", pure_virtual(&hp::RecordReader::close))
    ;

  class_<wrap_record_writer, std::auto_ptr<wrap_record_writer>, boost::noncopyable>("RecordWriter", "Basic wrapping of RecordWriter class")
    .def("emit", pure_virtual(&hp::RecordWriter::emit))
    .def("close", pure_virtual(&hp::RecordWriter::close))
    ;

  class_<wrap_factory, boost::noncopyable>("Factory")
    .def("createMapper", pure_virtual(&hp::Factory::createMapper),
	 return_value_policy<reference_existing_object>())
    .def("createReducer", pure_virtual(&hp::Factory::createReducer),
	 return_value_policy<reference_existing_object>())
    .def("createRecordReader", pure_virtual(&hp::Factory::createRecordReader),
	 return_value_policy<reference_existing_object>())
    .def("createCombiner", pure_virtual(&hp::Factory::createCombiner),
	 return_value_policy<reference_existing_object>())
    .def("createPartitioner", pure_virtual(&hp::Factory::createPartitioner),
	 return_value_policy<reference_existing_object>())
    .def("createRecordWriter", pure_virtual(&hp::Factory::createRecordWriter),
	 return_value_policy<reference_existing_object>())
    ;
  def("runTask", hp::runTask);
}
