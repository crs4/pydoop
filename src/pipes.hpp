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

class pipes_exception: public std::exception {
private:
  const std::string msg_;
public:
  pipes_exception(std::string msg) : msg_(msg){}

  virtual const char* what() const throw() {
    return msg_.c_str();
  }
  ~pipes_exception() throw() {}
} ;


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
    // t = (bool got_record, str key, str value)
    bp::tuple t = this->get_override("next")();
    if (!t[0]) {
      return false;
    }
    bp::extract<std::string> k(t[1]);
    if (k.check()) {
      key = k;
    } else {
      throw pipes_exception("RecordReader:: overloaded method is not returning a proper key.");
    }
    bp::extract<std::string> v(t[2]);
    if (v.check()) {
      value = v;
    } else {
      throw pipes_exception("RecordReader:: verloaded method is not returning a proper value.");
    }
    return true;
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

#define CREATE_AND_RETURN_OBJECT(obj_t, ctx_t, method_name, ctx) \
    bp::reference_existing_object::apply<obj_t&>::type converter; \
    PyObject* obj = converter(ctx); \
    bp::object po = bp::object(bp::handle<>(bp::borrowed(obj))); \
    bp::override f = this->get_override(#method_name); \
    if (f) {\
      obj_t* o = f(po); \
      return o;\
    } else {\
      return NULL;\
    }

struct wrap_factory: hp::Factory, bp::wrapper<hp::Factory> {
  //----------------------------------------------------------
  hp::Mapper* createMapper(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(hp::Mapper, hp::MapContext, createMapper, ctx);
  }
  hp::Reducer* createReducer(hp::ReduceContext& ctx) const{
    CREATE_AND_RETURN_OBJECT(hp::Reducer, hp::ReduceContext, createReducer, ctx);    
  }
  //----------------------------------------------------------
  hp::RecordReader* createRecordReader(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(hp::RecordReader, hp::MapContext, createRecordReader, ctx);
  }
  //----------------------------------------------------------
  hp::Reducer* createCombiner(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(hp::Reducer, hp::MapContext, createCombiner, ctx);    
  }
  //----------------------------------------------------------
  hp::Partitioner* createPartitioner(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(hp::Partitioner, hp::MapContext, createPartitioner, ctx);    
  }
  //----------------------------------------------------------
  hp::RecordWriter* createRecordWriter(hp::ReduceContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(hp::RecordWriter, hp::ReduceContext, createRecordWriter, ctx);    
  }

};


#endif  // HADOOP_PIPES_HPP

