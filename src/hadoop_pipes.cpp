#include <hadoop/Pipes.hh>

#include <boost/python.hpp>
using namespace boost::python;

#include "hadoop_pipes.hpp"

void try_mapper(Mapper& m, MapContext& mc){
  std::cerr << "** In try_mapper" << std::endl;
  m.map(mc);
}
void try_reducer(Reducer& r, ReduceContext& rc){
  std::cerr << "** In try_reducer" << std::endl;
  r.reduce(rc);
}

void try_factory(Factory& f, MapContext& mc, ReduceContext& rc){
  Mapper* m = f.createMapper(mc);
  m->map(mc);
  Reducer* r = f.createReducer(rc);
  r->reduce(rc);
}

class JobConfDummy: public JobConf {
  std::string s;
public:
  JobConfDummy() : s("ehehe"){}
  bool hasKey(const std::string& k) const { return false; };
  const std::string& get(const std::string& k) const{return s; };
  int getInt(const std::string& k) const{ return 1; };
  float getFloat(const std::string& k) const{return 0.2; };
  bool getBoolean(const std::string& k) const{return true; };
};

class TaskContextDummy: public MapContext, 
			public ReduceContext {
  std::string s;
  JobConfDummy *jobconf;
public:
  TaskContextDummy() : s("inputKey"){
    jobconf = new JobConfDummy();
  }
  const JobConf* getJobConf() {
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
  
  void incrementCounter(const TaskContext::Counter* c, 
			uint64_t i) {}
};

void try_factory_internal(Factory& f){
  
  TaskContextDummy tc;
  MapContext* mtcp = &(tc);
  std::cerr << "** try_factory_internal: 1" << std::endl;
  Mapper* m = f.createMapper(*mtcp);
  std::cerr << "** try_factory_internal: 2" << std::endl;
  m->map(tc);
  std::cerr << "** try_factory_internal: 3" << std::endl;
  Reducer* r = f.createReducer(tc);
  std::cerr << "** try_factory_internal: 4" << std::endl;
  r->reduce(tc);
  std::cerr << "** try_factory_internal: 5" << std::endl;
}


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
  def("try_mapper", try_mapper);
  def("try_reducer", try_reducer);
  def("try_factory", try_factory);
  def("try_factory_internal", try_factory_internal);
}








