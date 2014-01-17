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

#ifndef HADOOP_PIPES_HPP
#define HADOOP_PIPES_HPP

// Python 2.6 macro. Needed when compiling with older versions.
#ifndef Py_REFCNT
#define Py_REFCNT(ob) (((PyObject*)(ob))->ob_refcnt)
#endif

#include <string>

#include <hadoop/SerialUtils.hh>
#include "pipes_context.hpp"
#include "exceptions.hpp"

namespace hu = HadoopUtils;
namespace hp = HadoopPipes;


struct cxx_capsule {

  cxx_capsule() {
    in_cxx_land = false;
  };
  
  void entering_cxx_land(bool flag=true) {
    in_cxx_land = flag;
  }

  bool in_cxx_land;

};


#define DESTROY_PYTHON_TOO(wobj_t)                                   \
    if (in_cxx_land) {                                               \
      bp::reference_existing_object::apply<wobj_t*>::type converter; \
      PyObject* self = converter(this);                              \
      while (Py_REFCNT(self) > 0){Py_DECREF(self);}                  \
    }


struct wrap_mapper: hp::Mapper, bp::wrapper<hp::Mapper>, cxx_capsule {

  void map(hp::MapContext& ctx) {
    bp::reference_existing_object::apply<hp::MapContext&>::type converter;
    PyObject* obj = converter(ctx);
    bp::object po = bp::object(bp::handle<>(obj));
    this->get_override("map")(po);
    while (Py_REFCNT(obj) > 0){Py_DECREF(obj);}   
  }
  void close() {
    this->get_override("close")();
  }
  virtual ~wrap_mapper() {
    DESTROY_PYTHON_TOO(wrap_mapper);
  }
};

struct wrap_reducer: hp::Reducer, bp::wrapper<hp::Reducer>, cxx_capsule {
  void reduce(hp::ReduceContext& ctx) {
    bp::reference_existing_object::apply<hp::ReduceContext&>::type converter;
    PyObject* obj = converter(ctx);
    bp::object po = bp::object(bp::handle<>(obj));
    this->get_override("reduce")(po);
    while (Py_REFCNT(obj) > 0){Py_DECREF(obj);}   
  }
  void close() {
    this->get_override("close")();
  }
  virtual ~wrap_reducer() {
    DESTROY_PYTHON_TOO(wrap_reducer);
  }
};

struct wrap_partitioner: hp::Partitioner, bp::wrapper<hp::Partitioner>, cxx_capsule {
  int partition(const std::string& key, int numOfReduces) {
    return this->get_override("partition")(key, numOfReduces);
  }
  virtual ~wrap_partitioner() {
    DESTROY_PYTHON_TOO(wrap_partitioner);
  }
};

struct wrap_record_reader: hp::RecordReader, bp::wrapper<hp::RecordReader>, cxx_capsule {
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
      throw pipes_exception("RecordReader:: overloaded method is not returning a proper value.");
    }
    return true;
  }
  float getProgress() {
    return this->get_override("getProgress")();
  }
  void close() {
    this->get_override("close")();
  }
  virtual ~wrap_record_reader() {
    DESTROY_PYTHON_TOO(wrap_record_reader);
  }
};

struct wrap_record_writer: hp::RecordWriter, bp::wrapper<hp::RecordWriter>, cxx_capsule {
  void emit(const std::string& key, const std::string& value) {
    this->get_override("emit")(key, value);
  }
  void close() {
    this->get_override("close")();
  }
  virtual ~wrap_record_writer() {
    DESTROY_PYTHON_TOO(wrap_record_writer);
  }
};

#define CREATE_AND_RETURN_OBJECT(wobj_t, obj_t, ctx_t, method_name, ctx) \
  bp::reference_existing_object::apply<ctx_t&>::type converter;          \
  PyObject* po_ctx = converter(ctx);                                     \
  bp::object o_ctx = bp::object(bp::handle<>(po_ctx));                   \
  bp::override f = this->get_override(#method_name);                     \
  if (f) {                                                               \
    bp::object res = f(o_ctx);                                           \
    std::auto_ptr<wobj_t> ap = bp::extract<std::auto_ptr<wobj_t> >(res); \
    PyObject* obj = res.ptr();                                           \
    bp::incref(obj);                                                     \
    wobj_t* o = ap.get();                                                \
    ap.release();                                                        \
    o->entering_cxx_land();                                              \
    return o;                                                            \
  } else {                                                               \
    return NULL;                                                         \
  }

struct wrap_factory: hp::Factory, bp::wrapper<hp::Factory> {
  hp::Mapper* createMapper(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(wrap_mapper, hp::Mapper, hp::MapContext, 
                             createMapper, ctx);
  }

  hp::Reducer* createReducer(hp::ReduceContext& ctx) const{
    CREATE_AND_RETURN_OBJECT(wrap_reducer, hp::Reducer, hp::ReduceContext, 
                             createReducer, ctx);    
  }

  hp::RecordReader* createRecordReader(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(wrap_record_reader, hp::RecordReader,
                             hp::MapContext, createRecordReader, ctx);
  }

  hp::Reducer* createCombiner(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(wrap_reducer, hp::Reducer, hp::MapContext, 
                             createCombiner, ctx);    
  }

  hp::Partitioner* createPartitioner(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(wrap_partitioner, hp::Partitioner, hp::MapContext, 
                             createPartitioner, ctx);    
  }

  hp::RecordWriter* createRecordWriter(hp::ReduceContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(wrap_record_writer, hp::RecordWriter,
                             hp::ReduceContext, createRecordWriter, ctx);    
  }

};

#endif // HADOOP_PIPES_HPP
