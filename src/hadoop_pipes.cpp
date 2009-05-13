#include <hadoop/Pipes.hh>

// Disable original implementation of wrapper.
#define WRAPPER_BASE_DWA2004722_HPP
#define WRAPPER_DWA2004720_HPP

#include "hacked_wrapper.hpp"

#include <boost/python.hpp>

#include "hadoop_pipes.hpp"




#include <iostream>


namespace hp = HadoopPipes;
namespace bp = boost::python;



void try_mapper(hp::Mapper& m, hp::MapContext& mc){
  std::cerr << "** In try_mapper" << std::endl;
  m.map(mc);
}
void try_reducer(hp::Reducer& r, hp::ReduceContext& rc){
  std::cerr << "** In try_reducer" << std::endl;
  r.reduce(rc);
}

void try_factory(hp::Factory& f, hp::MapContext& mc, hp::ReduceContext& rc){
  hp::Mapper* m = f.createMapper(mc);
  m->map(mc);
  hp::Reducer* r = f.createReducer(rc);
  r->reduce(rc);
}

class JobConfDummy: public hp::JobConf {
  std::string s;
public:
  JobConfDummy() : s("ehehe"){}
  bool hasKey(const std::string& k) const { return false; };
  const std::string& get(const std::string& k) const{return s; };
  int getInt(const std::string& k) const{ return 1; };
  float getFloat(const std::string& k) const{return 0.2; };
  bool getBoolean(const std::string& k) const{return true; };
};

class TaskContextDummy: public hp::MapContext, 
			public hp::ReduceContext {
  std::string s;
  JobConfDummy *jobconf;
public:
  TaskContextDummy() : s("inputKey"){
    jobconf = new JobConfDummy();
  }
  const hp::JobConf* getJobConf() {
    return jobconf;
  }
  const std::string& getInputKey() {
    return s;
  }
  const std::string& getInputValue() {
    return s;
  }
  void emit(const std::string& k, const std::string& v) {
    std::cerr << "emit k=" << k << " v="<< v << std::endl;
  }
  void progress() {}
  void setStatus(const std::string& k) {}
  Counter* getCounter(const std::string& k, 
		      const std::string& v) {
    return new Counter(1);
  }
  bool nextValue() {}
  const std::string& getInputSplit()      { return s;}
  const std::string& getInputKeyClass()   { return s;}
  const std::string& getInputValueClass() { return s;}
  
  void incrementCounter(const hp::TaskContext::Counter* c, 
			uint64_t i) {}
};

void try_factory_internal(hp::Factory& f){
  
  TaskContextDummy tc;
  hp::MapContext* mtcp = &(tc);
  std::cerr << "** try_factory_internal: 1" << std::endl;
  hp::Mapper* m = f.createMapper(*mtcp);
  std::cerr << "** try_factory_internal: 2" << std::endl;
  m->map(tc);
  std::cerr << "** try_factory_internal: 3" << std::endl;
  hp::Reducer* r = f.createReducer(tc);
  std::cerr << "** try_factory_internal: 4" << std::endl;
  r->reduce(tc);
  std::cerr << "** try_factory_internal: 5" << std::endl;
}


//+++++++++++++++++++++++++++++++++++++++++
// Exporting class definitions.
//+++++++++++++++++++++++++++++++++++++++++
void export_hadoop_pipes() 
{
  using namespace boost::python;
  //--
  class_<wrap_mapper, boost::noncopyable>("Mapper",
					  "Basic wrapping of mapper class")
    .def("map", pure_virtual(&hp::Mapper::map))
    ;

  //--
  class_<wrap_reducer, boost::noncopyable>("Reducer",
					   "Basic wrapping of reducer class")
    .def("reduce", pure_virtual(&hp::Reducer::reduce))
    ;
  //--
  class_<wrap_partitioner, boost::noncopyable>("Partitioner"
					       "Basic wrapping of Partitioner class")

    .def("partition", pure_virtual(&hp::Partitioner::partition))
    ;
  //--
  class_<wrap_record_reader, boost::noncopyable>("RecordReader"
						 "Basic wrapping of RecordReader class")
    .def("next", pure_virtual(&hp::RecordReader::next))
    .def("getProgress", pure_virtual(&hp::RecordReader::getProgress))
    ;
  //--
  class_<wrap_record_writer, boost::noncopyable>("RecordWriter"
						 "Basic wrapping of RecordWriter class")
    .def("emit", pure_virtual(&hp::RecordWriter::emit))
    ;
  //--
  class_<wrap_factory, boost::noncopyable>("Factory")
    .def("createMapper", pure_virtual(&hp::Factory::createMapper),
	 return_value_policy<manage_new_object>())
    .def("createReducer", pure_virtual(&hp::Factory::createReducer),
	 return_value_policy<manage_new_object>())
    .def("createCombiner", 
	 &hp::Factory::createCombiner, &wrap_factory::default_create_combiner,
	 return_value_policy<manage_new_object>())
    .def("createPartitioner", 
	 &hp::Factory::createPartitioner, &wrap_factory::default_create_partitioner,
	 return_value_policy<manage_new_object>())
    .def("createRecordReader", 
	 &hp::Factory::createRecordReader, 
	 &wrap_factory::default_create_record_reader,
	 return_value_policy<manage_new_object>())
    .def("createRecordWriter", 
	 &hp::Factory::createRecordWriter, 
	 &wrap_factory::default_create_record_writer,
	 return_value_policy<manage_new_object>())
    ;
  def("runTask", hp::runTask);
  def("try_mapper", try_mapper);
  def("try_reducer", try_reducer);
  def("try_factory", try_factory);
  def("try_factory_internal", try_factory_internal);

}








