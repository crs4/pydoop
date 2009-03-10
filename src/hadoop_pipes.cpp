#include <hadoop/Pipes.hh>

#include <boost/python.hpp>
using namespace boost::python;

#include "hadoop_pipes.hpp"

//+++++++++++++++++++++++++++++++++++++++++
// Exporting class definitions.
//+++++++++++++++++++++++++++++++++++++++++
void export_hadoop_pipes() 
{
  //--
  class_<wrap_mapper, boost::noncopyable>("Mapper")
    .def("map", pure_virtual(&Mapper::map))
    ;
  //--
  class_<wrap_reducer, boost::noncopyable>("Reducer")
    .def("reduce", pure_virtual(&Reducer::reduce))
    ;
  //--
  class_<wrap_partitioner, boost::noncopyable>("Partitioner")
    .def("partition", pure_virtual(&Partitioner::partition))
    ;
  //--
  class_<wrap_record_reader, boost::noncopyable>("RecordReader")
    .def("next", pure_virtual(&RecordReader::next))
    .def("getProgress", pure_virtual(&RecordReader::getProgress))
    ;
  //--
  class_<wrap_record_writer, boost::noncopyable>("RecordWriter")
    .def("emit", pure_virtual(&RecordWriter::emit))
    ;
  //--
  class_<wrap_factory, boost::noncopyable>("Factory")
    .def("createMapper", pure_virtual(&Factory::createMapper),
	 return_value_policy<manage_new_object>())
    .def("createReducer", pure_virtual(&Factory::createReducer),
	 return_value_policy<manage_new_object>())
    .def("createCombiner", 
	 &Factory::createCombiner, &wrap_factory::default_create_combiner,
	 return_value_policy<manage_new_object>())
    .def("createPartitioner", 
	 &Factory::createPartitioner, &wrap_factory::default_create_partitioner,
	 return_value_policy<manage_new_object>())
    .def("createRecordReader", 
	 &Factory::createRecordReader, 
	 &wrap_factory::default_create_record_reader,
	 return_value_policy<manage_new_object>())
    .def("createRecordWriter", 
	 &Factory::createRecordWriter, 
	 &wrap_factory::default_create_record_writer,
	 return_value_policy<manage_new_object>())
    ;
  def("runTask", runTask);
}








