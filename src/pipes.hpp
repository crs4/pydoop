#ifndef HADOOP_PIPES_HPP
#define HADOOP_PIPES_HPP


#include "pipes_context.hpp"

#include <hadoop/SerialUtils.hh>
namespace hu = HadoopUtils;
namespace hp = HadoopPipes;


#include <iostream>
#include <fstream>
#include <ios>

#include <string>

struct wrap_mapper: hp::Mapper, bp::wrapper<hp::Mapper> {

  void map(hp::MapContext& ctx) {
    bp::reference_existing_object::apply<hp::MapContext&>::type converter;
    PyObject* obj = converter(ctx);
    bp::object po = bp::object(bp::handle<>(bp::borrowed(obj)));
    this->get_override("map")(po);
  }
  virtual ~wrap_mapper() {
    std::cerr << "~wrap_mapper: decrementing...." << std::endl;
    std::cerr << "~wrap_mapper: " << m_self << std::endl;
    std::cerr << "~wrap_mapper: ob_refcnt " << (m_self)->ob_refcnt << std::endl;
    //Py_XDECREF(m_self);
    std::cerr << "~wrap_mapper: done." << std::endl;
  }
};

struct wrap_reducer: hp::Reducer, bp::wrapper<hp::Reducer> {
  void reduce(hp::ReduceContext& ctx) {
    bp::reference_existing_object::apply<hp::ReduceContext&>::type converter;
    PyObject* obj = converter(ctx);
    bp::object po = bp::object(bp::handle<>(bp::borrowed(obj)));
    this->get_override("reduce")(po);
  }
  ~wrap_reducer() {
    std::cerr << "~wrap_reducer" << std::endl;
  }
};

struct wrap_partitioner: hp::Partitioner, bp::wrapper<hp::Partitioner> {
  int partition(const std::string& key, int numOfReduces) {
    return this->get_override("partition")(key, numOfReduces);
  }
  ~wrap_partitioner() {
    std::cerr << "~wrap_partitioner" << std::endl;
  }
};

struct wrap_record_reader: hp::RecordReader, bp::wrapper<hp::RecordReader> {
  bool next(std::string& key, std::string& value) {
    return this->get_override("next")(key, value);
  }
  float getProgress() {
    return this->get_override("getProgress")();
  }
  ~wrap_record_reader() {
    std::cerr << "~wrap_record_reader" << std::endl;
  }
};

struct wrap_record_writer: hp::RecordWriter, bp::wrapper<hp::RecordWriter> {
  void emit(const std::string& key, const std::string& value) {
    this->get_override("emit")(key, value);
  }
  ~wrap_record_writer() {
    std::cerr << "~wrap_record_writer" << std::endl;
  }
};

#define CREATE_AND_RETURN_OBJECT(obj_t, ctx_t, name, arg) \
    bp::reference_existing_object::apply<obj_t&>::type converter; \
    PyObject* obj = converter(arg); \
    bp::object po = bp::object(bp::handle<>(bp::borrowed(obj))); \
    bp::override f = this->get_override("create"#name); \
    obj_t* o = f(po); \
    return o;


struct wrap_factory: hp::Factory, bp::wrapper<hp::Factory> {
  //----------------------------------------------------------
  hp::Mapper* createMapper(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(hp::Mapper, hp::MapContext, Mapper, ctx);
  }
  hp::Reducer* createReducer(hp::ReduceContext& ctx) const{
    CREATE_AND_RETURN_OBJECT(hp::Reducer, hp::ReduceContext, Reducer, ctx);    
  }

  // FIXME ------ the following are probably broken.
#define OVERRIDE_CREATOR_IF_POSSIBLE(base, ctx_type, method_name, arg)	\
  if (bp::override f = this->get_override(#method_name)) {		\
    const ctx_type& c_ctx = arg;\
    bp::object o_ctx = bp::make_getter(&c_ctx,				\
				       bp::return_value_policy<bp::copy_const_reference>()); \
    return f(o_ctx); \
  } else { \
    return base::method_name(arg); \
  }

  //----------------------------------------------------------
  hp::Reducer* createCombiner(hp::MapContext& ctx) const {
    OVERRIDE_CREATOR_IF_POSSIBLE(hp::Factory, hp::MapContext, 
				 createCombiner, ctx);
  }
  hp::Reducer* default_create_combiner(hp::MapContext& ctx) const {
    return this->Factory::createCombiner(ctx);
  }
  //----------------------------------------------------------
  hp::Partitioner* createPartitioner(hp::MapContext& ctx) const {
    OVERRIDE_CREATOR_IF_POSSIBLE(hp::Factory, hp::MapContext,
				 createPartitioner, ctx);
  }
  hp::Partitioner* default_create_partitioner(hp::MapContext& ctx) const {
    return this->Factory::createPartitioner(ctx);
  }
  //----------------------------------------------------------
  hp::RecordWriter* createRecordWriter(hp::ReduceContext& ctx) const {
    OVERRIDE_CREATOR_IF_POSSIBLE(hp::Factory, hp::ReduceContext,
				 createRecordWriter, ctx);
  }
  hp::RecordWriter* default_create_record_writer(hp::ReduceContext& ctx) const {
    return this->Factory::createRecordWriter(ctx);
  }
  //----------------------------------------------------------
  hp::RecordReader* createRecordReader(hp::MapContext& ctx) const {
    OVERRIDE_CREATOR_IF_POSSIBLE(hp::Factory, hp::MapContext,
				 createRecordReader, ctx);
  }
  hp::RecordReader* default_create_record_reader(hp::MapContext& ctx) const {
    return this->Factory::createRecordReader(ctx);
  }
};


#endif  // HADOOP_PIPES_HPP

