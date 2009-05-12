#ifndef HADOOP_PIPES_TEST_SUPPORT_HPP
#define HADOOP_PIPES_TEST_SUPPORT_HPP

#include <string>
#include <vector>
#include <map>
#include <iostream>


#include "hadoop/StringUtils.hh"
#include "hadoop/SerialUtils.hh"

using namespace HadoopPipes;
using namespace HadoopUtils;

struct test_factory {
  Factory& f_;
  test_factory(Factory& f): f_(f){}
  Mapper* createMapper(MapContext& ctx) {
    std::cerr << "test_factory:: Ready to evaluate createMapper" << std::endl;
    Mapper* m = f_.createMapper(ctx);
    std::cerr << "test_factory:: done." << std::endl;
    return m;
  }
  Reducer* createReducer(ReduceContext& ctx) {
    return f_.createReducer(ctx);
  }
};

//-----------------------------------------------------------------------------------//
class JobConfImpl: public JobConf {
private:
  std::map<std::string, std::string> values;

public:
  void set(const std::string& key, const std::string& value) {
    values[key] = value;
  }
  virtual bool hasKey(const std::string& key) const {
    return values.find(key) != values.end();
  }
  virtual const std::string& get(const std::string& key) const {
    std::map<std::string,std::string>::const_iterator itr = values.find(key);
    if (itr == values.end()) {
      throw Error("Key " + key + " not found in JobConf");
    }
    return itr->second;
  }

  virtual int getInt(const std::string& key) const {
    const std::string& val = get(key);
    return toInt(val);
  }
  
  virtual float getFloat(const std::string& key) const {
    const std::string& val = get(key);
    return toFloat(val);
  }

  virtual bool getBoolean(const std::string&key) const {
    const std::string& val = get(key);
    return toBool(val);
  }
};
//-----------------------------------------------------------------------------------//
class TaskContextImpl: public TaskContext {
private:
  JobConfImpl      job_conf;
  std::string      input_key;
  std::string      input_value;
  std::vector<int> counter_vals;
  
public:
  TaskContextImpl(const std::string& ik, const std::string& iv) :
    job_conf(), input_key(ik), input_value(iv) { }
  //
  virtual const JobConf* getJobConf()        { return &job_conf;}
  virtual const std::string& getInputKey()   { return input_key;}
  virtual const std::string& getInputValue() { return input_value;}
  virtual void  emit(const std::string& k, const std::string& v) {
    std::cerr << "TaskContextImpl::emit("<<k<<", "<<v<<")"<<std::endl;
  }
  virtual void  progress() {
    std::cerr << "TaskContextImpl::progress()"<<std::endl;
  }
  virtual void  setStatus(const std::string& status) {
    std::cerr << "TaskContextImpl::setStatus("<<status<<")"<<std::endl;
  }
  virtual Counter* getCounter(const std::string& group, const std::string& name) {
    int id = counter_vals.size();
    counter_vals.push_back(0);
    return new Counter(id);
  }
  virtual void incrementCounter(const Counter* counter, uint64_t amount) {
    int id = counter->getId();
    counter_vals[id] += amount;
    std::cerr << "TaskContextImpl::incrementCounter("<<id<<", "
	      <<amount<<")->"<< counter_vals[id]<< std::endl;
  }
};

//-----------------------------------------------------------------------------------//
//-----------------------------------------------------------------------------------//
//-----------------------------------------------------------------------------------//
JobConf* wrap_JobConf_object(JobConf& jc){ return &jc; }
JobConf* get_JobConf_object(dict d){
  JobConfImpl* jc = new JobConfImpl();
  list keylist     = d.keys();
  int  keylist_len = extract<int>(keylist.attr("__len__")());
  for(int i = 0; i < keylist_len; ++i){
    std::string k = extract<std::string>(keylist[i]);
    std::string v = extract<std::string>(d[keylist[i]]);
    jc->set(k, v);
  }
  return jc;
}

TaskContext* get_TaskContext_object(dict d){
  std::string ik = extract<std::string>(d["input_key"]);
  std::string iv = extract<std::string>(d["input_value"]);
  TaskContext* tc = new TaskContextImpl(ik, iv);
  return tc;
}

#if 0
void try_context(TaskContext& tc){
  std::cerr << "** in try_contxt"                        << std::endl;
  std::cerr << "Inputkey="    << tc.getInputKey()        << std::endl;
  std::cerr << "InputValue="  << tc.getInputValue()      << std::endl;
  const std::string& k = tc.getInputKey();
  const std::string& v = tc.getInputValue();
  std::cerr << "** trying emit(" << k << "," << v << ")" <<std::endl;
  tc.emit(k, v);
}
#endif


#endif
 
