#ifndef HADOOP_PIPES_HPP
#define HADOOP_PIPES_HPP


#include "hadoop_pipes_context.hpp"

#include <hadoop/SerialUtils.hh>
namespace hu = HadoopUtils;
namespace hp = HadoopPipes;


#include <iostream>
#include <fstream>
#include <ios>

#include <string>

// lifted from pycuda...

template <typename T>
inline boost::python::handle<> handle_from_new_ptr(T *ptr){
  return boost::python::handle<>(
				 typename boost::python::manage_new_object::apply<T *>::type()(ptr));
}

template <typename T>
inline boost::python::handle<> handle_from_old_ptr(T *ptr){
  return boost::python::handle<>(
				 typename boost::python::reference_existing_object::apply<T *>::type()(ptr));
}


struct wrap_mapper: hp::Mapper, bp::wrapper<hp::Mapper> {

  void map(hp::MapContext& ctx) {
    std::cerr << "wrap_mapper::map ctx=" << &(ctx) << std::endl;
    bp::override f = this->get_override("map");
#if 1
    bp::reference_existing_object::apply<hp::MapContext&>::type converter;
    PyObject* obj = converter(ctx);
    bp::object po = bp::object(bp::handle<>(obj));
    f(po);    
    //this->get_override("map")(po);
#else
    bp::object o_ctx = bp::make_tuple(handle_from_old_ptr(&ctx))[0];
    std::cerr << "Got a o_ctx " << std::endl;
    f(o_ctx);
    std::cerr << "ready to leave map." << std::endl;
#endif
  }
  virtual ~wrap_mapper() {
    std::cerr << "~wrap_mapper: decrementing...." << std::endl;
    std::cerr << "~wrap_mapper: " << m_self << std::endl;
    std::cerr << "~wrap_mapper: ob_refcnt" << (m_self)->ob_refcnt << std::endl;
    //Py_XDECREF(m_self);
    std::cerr << "~wrap_mapper: done." << std::endl;
  }
};

struct wrap_reducer: hp::Reducer, bp::wrapper<hp::Reducer> {
  void reduce(hp::ReduceContext& ctx) {
    bp::override f = this->get_override("reduce");
    bp::object o_ctx = make_tuple(handle_from_old_ptr(&ctx))[0];
    f(o_ctx);
  }
  ~wrap_reducer() {
    std::cerr << "~wrap_reducer" << std::endl;
  }
  PyObject* _py_self;
};

struct wrap_partitioner: hp::Partitioner, bp::wrapper<hp::Partitioner> {
  int partition(const std::string& key, int numOfReduces) {
    return this->get_override("partition")(key, numOfReduces);
  }
  ~wrap_partitioner() {
    std::cerr << "~wrap_partitioner" << std::endl;
  }
  PyObject* _py_self;
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

  PyObject* _py_self;
};

struct wrap_record_writer: hp::RecordWriter, bp::wrapper<hp::RecordWriter> {
  void emit(const std::string& key, const std::string& value) {
    this->get_override("emit")(key, value);
  }
  ~wrap_record_writer() {
    std::cerr << "~wrap_record_writer" << std::endl;
  }

  PyObject* _py_self;
};

#define OVERRIDE_CREATOR_IF_POSSIBLE(base, ctx_type, method_name, arg)	\
  if (bp::override f = this->get_override(#method_name)) {		\
    const ctx_type& c_ctx = arg;\
    bp::object o_ctx = bp::make_getter(&c_ctx,				\
				       bp::return_value_policy<bp::copy_const_reference>()); \
    return f(o_ctx); \
  } else { \
    return base::method_name(arg); \
  }


struct wrap_factory: hp::Factory, bp::wrapper<hp::Factory> {
  //----------------------------------------------------------
  hp::Mapper* createMapper(hp::MapContext& ctx) const {
#if 0
    const hp::MapContext& c_ctx = ctx;
    bp::object o_ctx = bp::make_getter(&c_ctx, 
				       bp::return_value_policy<copy_const_reference>());
    return this->get_override("createMapper")(o_ctx);
#else
    std::cerr << "createMapper() wrap_factory:: ctx=" << &(ctx) << std::endl;
    bp::override f = this->get_override("createMapper");
    bp::reference_existing_object::apply<hp::MapContext&>::type converter;
    PyObject* obj = converter(ctx);
    bp::object po = bp::object(bp::handle<>(bp::borrowed(obj)));
    hp::Mapper* m = f(po);
    std::cerr << "createMapper() Got a mapper " << m << std::endl;
    return m;
#endif

  }
  hp::Reducer* createReducer(hp::ReduceContext& ctx) const{
    //    OVERRIDE_CREATOR_IF_POSSIBLE(hp::Factory, hp::ReduceContext, 
    //				 createReducer, ctx);
    const hp::ReduceContext& c_ctx = ctx;
    bp::object o_ctx = bp::make_getter(&c_ctx, 
				       bp::return_value_policy<bp::copy_const_reference>());
    return this->get_override("createReducer")(o_ctx);

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

