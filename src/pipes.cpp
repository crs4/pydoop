// BEGIN_COPYRIGHT
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
    ;

  class_<wrap_reducer, std::auto_ptr<wrap_reducer>, boost::noncopyable>("Reducer", "Basic wrapping of reducer class")
    .def("reduce", pure_virtual(&hp::Reducer::reduce))
    ;

  class_<wrap_partitioner, std::auto_ptr<wrap_partitioner>, boost::noncopyable>("Partitioner", "Basic wrapping of Partitioner class")
    .def("partition", pure_virtual(&hp::Partitioner::partition))
    ;

  class_<wrap_record_reader, std::auto_ptr<wrap_record_reader>, boost::noncopyable>("RecordReader", "Basic wrapping of RecordReader class")
    // We disable python view of next, since it is unclear how its
    // call by ref to implement a side-effect should be interpreted.
    //.def("next",pure_virtual(&hp::RecordReader::next))
    .def("getProgress", pure_virtual(&hp::RecordReader::getProgress))
    ;

  class_<wrap_record_writer, std::auto_ptr<wrap_record_writer>, boost::noncopyable>("RecordWriter", "Basic wrapping of RecordWriter class")
    .def("emit", pure_virtual(&hp::RecordWriter::emit))
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
