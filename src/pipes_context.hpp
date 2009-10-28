// BEGIN_COPYRIGHT
// END_COPYRIGHT
#ifndef HADOOP_PIPES_CONTEXT_HPP
#define HADOOP_PIPES_CONTEXT_HPP


#include <string>
#include <vector>

#include <hadoop/Pipes.hh>
#include <boost/python.hpp>


namespace bp = boost::python;
namespace hp = HadoopPipes;


#define OVERRIDE_STR_REF(method_name)                      \
  if (bp::override f = this->get_override(method_name)) {  \
    bp::str r = f();                                       \
    bp::incref(bp::object(r).ptr());                       \
    return bp::extract<const std::string&>(r);             \
 } else {                                                  \
    return bp::extract<const std::string&>(bp::object());  \
 }

#define OVERRIDE_STR_REF_1(method_name,a)                  \
  if (bp::override f = this->get_override(method_name)) {  \
    bp::str r = f(a);                                      \
    bp::incref(bp::object(r).ptr());                       \
    return bp::extract<const std::string&>(r);             \
 } else {                                                  \
    return bp::extract<const std::string&>(bp::object());  \
 }


//+++++++++++++++++++++++++++++++++++++++++//
//                JobConf                  //
//+++++++++++++++++++++++++++++++++++++++++//

struct wrap_job_conf : hp::JobConf, bp::wrapper<hp::JobConf> {
  bool hasKey(const std::string& k) const {
    return this->get_override("hasKey")(k);
  }
  const std::string& get(const std::string& k) const {
    OVERRIDE_STR_REF_1("get", k);
  }
  int getInt(const std::string& k) const {
    return this->get_override("getInt")(k);
  }
  float getFloat(const std::string& k) const {
    return this->get_override("getFloat")(k);
  }
  bool getBoolean(const std::string& k) const {
    return this->get_override("getBoolean")(k);
  }
};


//+++++++++++++++++++++++++++++++++++++++++//
//              TaskContext                //
//+++++++++++++++++++++++++++++++++++++++++//

struct wrap_task_context: hp::TaskContext, bp::wrapper<hp::TaskContext> {
  const hp::JobConf* getJobConf() {
    return this->get_override("getJobConf")();
  }
  const std::string& getInputKey()   {
    hp::TaskContext* tc = this;
    const std::string& r = tc->getInputKey();
    return r;
    //OVERRIDE_STR_REF("getInputKey");
  };
  const std::string& getInputValue() {
    hp::TaskContext* tc = this;
    const std::string& r = tc->getInputValue();
    return r;
    //OVERRIDE_STR_REF("getInputValue");
  };
  void  emit(const std::string& k, const std::string& v) {
    this->get_override("emit")(k, v);    
  };
  void  progress() {
    this->get_override("progress")();
  };
  void  setStatus(const std::string& status){
    this->get_override("setStatus")(status);
  };
  Counter* getCounter(const std::string& group, const std::string& name) {
    return this->get_override("getCounter")(group, name);
  };
  void incrementCounter(const Counter* counter, uint64_t amount) { 
    this->get_override("incrementCounter")(counter, amount);
  };
};


//+++++++++++++++++++++++++++++++++++++++++//
//              MapContext                 //
//+++++++++++++++++++++++++++++++++++++++++//

struct wrap_map_context: hp::MapContext, bp::wrapper<hp::MapContext> {
  const hp::JobConf* getJobConf() {
    return this->get_override("getJobConf")();
  }
  const std::string& getInputKey()   {
    OVERRIDE_STR_REF("getInputKey"); 
  };
  const std::string& getInputValue() {
    hp::MapContext* tc = this;
    const std::string& r = tc->getInputValue();
    return r;
    // OVERRIDE_STR_REF("getInputValue"); 
  };
  void  emit(const std::string& k, const std::string& v) {
    this->get_override("emit")(k, v);    
  };
  void  progress() {
    this->get_override("progress")();
  };
  void  setStatus(const std::string& status){
    this->get_override("setStatus")(status);
  };
  Counter* getCounter(const std::string& group, const std::string& name) {
    return this->get_override("getCounter")(group, name);
  };
  void incrementCounter(const Counter* counter, uint64_t amount) { 
    this->get_override("incrementCounter")(counter, amount);
  };
  //-------------------------
  const std::string& getInputSplit() {
    OVERRIDE_STR_REF("getInputSplit"); 
  }
  const std::string& getInputKeyClass() {
    OVERRIDE_STR_REF("getInputKeyClass"); 
  }
  const std::string& getInputValueClass() {
    OVERRIDE_STR_REF("getInputValueClass");
  }
};


//+++++++++++++++++++++++++++++++++++++++++//
//              ReduceContext              //
//+++++++++++++++++++++++++++++++++++++++++//

struct wrap_reduce_context: hp::ReduceContext, bp::wrapper<hp::ReduceContext> {
  const hp::JobConf* getJobConf() {
    return this->get_override("getJobConf")();
  }
  const std::string& getInputKey()   { 
    OVERRIDE_STR_REF("getInputKey"); 
  };
  const std::string& getInputValue() {
    OVERRIDE_STR_REF("getInputValue"); 
  };
  void  emit(const std::string& k, const std::string& v) {
    this->get_override("emit")(k, v);    
  };
  void  progress() {
    this->get_override("progress")();
  };
  void  setStatus(const std::string& status){
    this->get_override("setStatus")(status);
  };
  Counter* getCounter(const std::string& group, const std::string& name) {
    return this->get_override("getCounter")(group, name);
  };
  void incrementCounter(const Counter* counter, uint64_t amount) { 
    this->get_override("incrementCounter")(counter, amount);
  };
  //-------------------------
  bool nextValue() {
    return this->get_override("nextValue")();
  }
};

#endif // HADOOP_PIPES_CONTEXT_HPP
