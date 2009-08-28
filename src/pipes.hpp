#ifndef HADOOP_PIPES_HPP
#define HADOOP_PIPES_HPP

// borrowing a Python 2.6 macro
#ifndef Py_REFCNT
#define Py_REFCNT(ob)		(((PyObject*)(ob))->ob_refcnt)
#endif

#include "pipes_context.hpp"

#include <hadoop/SerialUtils.hh>
namespace hu = HadoopUtils;
namespace hp = HadoopPipes;

#include <iostream>
#include <fstream>
#include <ios>

#include <string>

#include "exceptions.hpp"

struct cxx_capsule {
  cxx_capsule() {
    std::cerr << "cxx_capsule:cxx_capsule" << std::endl;
    in_cxx_land = false;
  };
  
  void entering_cxx_land(bool flag=true){
    std::cerr << "cxx_capsule::entering_cxx_land flag=" << flag << std::endl;
    in_cxx_land = flag;
  }

  bool in_cxx_land;
};


#define DESTROY_PYTHON_TOO(wobj_t) \
    std::cerr << "~" #wobj_t ": invoked." << std::endl;\
    if (in_cxx_land) {\
      std::cerr << "       : we are in_cxx_land." << std::endl;\
      bp::reference_existing_object::apply<wobj_t*>::type converter;\
      PyObject* self = converter(this);\
      std::cerr << "       : current refcount is " << Py_REFCNT(self) << std::endl;\
      while (Py_REFCNT(self) > 0){Py_DECREF(self);}\
    }


struct wrap_mapper: hp::Mapper, bp::wrapper<hp::Mapper>, cxx_capsule {

  void map(hp::MapContext& ctx) {
    bp::reference_existing_object::apply<hp::MapContext&>::type converter;
    PyObject* obj = converter(ctx);
    bp::object po = bp::object(bp::handle<>(bp::borrowed(obj)));
    this->get_override("map")(po);
  }
  virtual ~wrap_mapper() {
#if 1
    DESTROY_PYTHON_TOO(wrap_mapper);
#else
    std::cerr << "~wrap_mapper: invoked." << std::endl;
    if (in_cxx_land) {
      std::cerr << "~wrap_mapper: we are in_cxx_land." << std::endl;
      bp::reference_existing_object::apply<wrap_mapper*>::type converter;
      PyObject* self = converter(this);
      std::cerr << "~wrap_mapper: current refcount is " << Py_REFCNT(self) << std::endl;
      Py_DECREF(self);
    }
#endif
  }
};

struct wrap_reducer: hp::Reducer, bp::wrapper<hp::Reducer>, cxx_capsule {
  void reduce(hp::ReduceContext& ctx) {
    bp::reference_existing_object::apply<hp::ReduceContext&>::type converter;
    PyObject* obj = converter(ctx);
    bp::object po = bp::object(bp::handle<>(bp::borrowed(obj)));
    this->get_override("reduce")(po);
  }
  ~wrap_reducer() {
    DESTROY_PYTHON_TOO(wrap_reducer);
  }
};

struct wrap_partitioner: hp::Partitioner, bp::wrapper<hp::Partitioner>, cxx_capsule {
  int partition(const std::string& key, int numOfReduces) {
    return this->get_override("partition")(key, numOfReduces);
  }
  ~wrap_partitioner() {
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
  ~wrap_record_reader() {
    DESTROY_PYTHON_TOO(wrap_record_reader);
  }
};

struct wrap_record_writer: hp::RecordWriter, bp::wrapper<hp::RecordWriter>, cxx_capsule {
  void emit(const std::string& key, const std::string& value) {
    this->get_override("emit")(key, value);
  }
  ~wrap_record_writer() {
    DESTROY_PYTHON_TOO(wrap_record_writer);
  }
};

#if 1
#define CREATE_AND_RETURN_OBJECT(wobj_t, obj_t, ctx_t, method_name, ctx) \
    bp::reference_existing_object::apply<ctx_t&>::type converter;\
    PyObject* po_ctx = converter(ctx);\
    bp::object o_ctx = bp::object(bp::handle<>(bp::borrowed(po_ctx)));\
    bp::override f = this->get_override(#method_name);\
    if (f) {\
      bp::object res = f(o_ctx);\
      std::auto_ptr<wobj_t> ap = bp::extract<std::auto_ptr<wobj_t> >(res);\
      PyObject* obj = res.ptr();\
      std::cerr << #method_name ": current refcount is " << Py_REFCNT(obj) << std::endl;\
      Py_INCREF(obj);\
      wobj_t* o = ap.get();\
      ap.release();\
      o->entering_cxx_land();\
      return o;\
    } else {\
      return NULL;\
    }
#else
#define CREATE_AND_RETURN_OBJECT(wobj_t, obj_t, ctx_t, method_name, ctx) \
    bp::reference_existing_object::apply<ctx_t&>::type converter;\
    PyObject* po_ctx = converter(ctx);\
    bp::object o_ctx = bp::object(bp::handle<>(bp::borrowed(po_ctx)));\
    bp::override f = this->get_override(#method_name);\
    if (f) {\
      bp::object res = f(o_ctx);\
      std::auto_ptr<wobj_t> ap = bp::extract<std::auto_ptr<wobj_t> >(res);\
      PyObject* obj = res.ptr();\
      std::cerr << #method_name ": current refcount is " << Py_REFCNT(obj) << std::endl;\
      wobj_t* o = ap.get();\
      ap.release();\
      o->entering_cxx_land();\
      return o;\
    } else {\
      return NULL;\
    }
#endif

struct wrap_factory: hp::Factory, bp::wrapper<hp::Factory> {
  //----------------------------------------------------------
  hp::Mapper* createMapper(hp::MapContext& ctx) const {
#if 0
    typedef hp::MapContext ctx_t;
    typedef wrap_mapper wobj_t;
    typedef hp::Mapper obj_t;
    bp::reference_existing_object::apply<ctx_t&>::type converter;
    PyObject* obj = converter(ctx);
    bp::object po = bp::object(bp::handle<>(bp::borrowed(obj)));
    bp::override f = this->get_override("createMapper");
    if (f) {
      bp::object res = f(po);
      std::auto_ptr<wobj_t> ap = bp::extract<std::auto_ptr<wobj_t> >(res);
      PyObject* obj = res.ptr();
      std::cerr << "createMapper: current refcount is " << Py_REFCNT(obj) << std::endl;
      Py_INCREF(obj);
      wobj_t* o = ap.get();
      ap.release();
      o->in_cxx_land = true;
      return o;
    } else {
      return NULL;
    }
#else
    CREATE_AND_RETURN_OBJECT(wrap_mapper, hp::Mapper, hp::MapContext, 
			     createMapper, ctx);    
#endif
  }
  hp::Reducer* createReducer(hp::ReduceContext& ctx) const{
    CREATE_AND_RETURN_OBJECT(wrap_reducer, hp::Reducer, hp::ReduceContext, 
			     createReducer, ctx);    
  }
  //----------------------------------------------------------
  hp::RecordReader* createRecordReader(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(wrap_record_reader, hp::RecordReader, hp::MapContext, 
			     createRecordReader, ctx);
  }
  //----------------------------------------------------------
  hp::Reducer* createCombiner(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(wrap_reducer, hp::Reducer, hp::MapContext, 
			     createCombiner, ctx);    
  }
  //----------------------------------------------------------
  hp::Partitioner* createPartitioner(hp::MapContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(wrap_partitioner, hp::Partitioner, hp::MapContext, 
			     createPartitioner, ctx);    
  }
  //----------------------------------------------------------
  hp::RecordWriter* createRecordWriter(hp::ReduceContext& ctx) const {
    CREATE_AND_RETURN_OBJECT(wrap_record_writer, hp::RecordWriter, hp::ReduceContext, 
			     createRecordWriter, ctx);    
  }

};


#endif  // HADOOP_PIPES_HPP

