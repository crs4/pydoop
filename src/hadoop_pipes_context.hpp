#ifndef HADOOP_PIPES_CONTEXT_HPP
#define HADOOP_PIPES_CONTEXT_HPP

#include <string>
#include <vector>
#include <iostream>

using namespace HadoopPipes;

static std::vector<std::string> horrible_hack;

#define OVERRIDE_STR_REF(method_name) \
 if (override f = this->get_override(method_name)){ \
    str r = f(); \
    horrible_hack.push_back(extract<std::string>(r)); \
    return horrible_hack.back(); \
 } else { \
   return extract<const std::string&>(object());	\
 }
#define OVERRIDE_STR_REF_1(method_name,a)	    \
 if (override f = this->get_override(method_name)){ \
    str r = f(a); \
    horrible_hack.push_back(extract<std::string>(r)); \
    return horrible_hack.back(); \
 } else { \
   return extract<const std::string&>(object());	\
 }


//+++++++++++++++++++++++++++++++++++++++++//
//                JobConf                  //
//+++++++++++++++++++++++++++++++++++++++++//

struct wrap_job_conf : JobConf,
		       wrapper<JobConf>{
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

struct wrap_task_context : TaskContext,
			   wrapper<TaskContext>{
  const JobConf* getJobConf() {
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
};


//+++++++++++++++++++++++++++++++++++++++++//
//              MapContext                //
//+++++++++++++++++++++++++++++++++++++++++//
struct wrap_map_context : MapContext,
			  wrapper<MapContext>{
  const JobConf* getJobConf() {
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
//              ReduceContext                //
//+++++++++++++++++++++++++++++++++++++++++//
struct wrap_reduce_context : ReduceContext,
			     wrapper<ReduceContext>{
  const JobConf* getJobConf() {
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
#endif
 
