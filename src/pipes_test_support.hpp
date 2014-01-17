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

#ifndef HADOOP_PIPES_TEST_SUPPORT_HPP
#define HADOOP_PIPES_TEST_SUPPORT_HPP

#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <sstream>

#include <hadoop/StringUtils.hh>
#include <hadoop/SerialUtils.hh>

#include <boost/python.hpp>

namespace hp = HadoopPipes;
namespace bp = boost::python;


void try_mapper(hp::Mapper& m, hp::MapContext& mc) {
  std::cerr << "** In try_mapper" << std::endl;
  m.map(mc);
}

void try_reducer(hp::Reducer& r, hp::ReduceContext& rc) {
  std::cerr << "** In try_reducer" << std::endl;
  r.reduce(rc);
}

void try_factory(hp::Factory& f, hp::MapContext& mc, hp::ReduceContext& rc) {
  hp::Mapper* m = f.createMapper(mc);
  m->map(mc);
  hp::Reducer* r = f.createReducer(rc);
  r->reduce(rc);
}


class JobConfDummy: public hp::JobConf {
  std::string s;
public:
  JobConfDummy() : s("dummy") {}
  bool hasKey(const std::string& k) const { return false; };
  const std::string& get(const std::string& k) const { return s; };
  int getInt(const std::string& k) const { return 1; };
  float getFloat(const std::string& k) const { return 0.2; };
  bool getBoolean(const std::string& k) const { return true; };
};

class TaskContextDummy: public hp::MapContext, public hp::ReduceContext {
  std::string s;
  JobConfDummy *jobconf;
public:
  TaskContextDummy() : s("inputKey") {
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
  Counter* getCounter(const std::string& k, const std::string& v) {
    return new Counter(1);
  }
  bool nextValue() {}
  const std::string& getInputSplit() { return s; }
  const std::string& getInputKeyClass() { return s; }
  const std::string& getInputValueClass() { return s; }
  
  void incrementCounter(const hp::TaskContext::Counter* c, uint64_t i) {}
};


void try_factory_internal(hp::Factory& f) {
  
  TaskContextDummy tc;
  hp::MapContext* mtcp = &(tc);
  std::cerr << "** try_factory_internal: 1 -- mapper(ctx)" << std::endl;
  hp::Mapper* m = f.createMapper(*mtcp);
  std::cerr << "** try_factory_internal: 2 -- mapper.map(ctx)" << std::endl;
  m->map(tc);
  std::cerr << "** try_factory_internal: 3 -- delete mapper " << m << std::endl;
  delete m;
  std::cerr << "** try_factory_internal: 4 -- after delete mapper " << std::endl;
  std::cerr << "** try_factory_internal: 3 -- reducer(ctx)" << std::endl;
  hp::Reducer* r = f.createReducer(tc);
  std::cerr << "** try_factory_internal: 4 -- reducer.reduce(ctx)" << std::endl;
  r->reduce(tc);
  std::cerr << "** try_factory_internal: 6 -- delete reducer" << r << std::endl;
  delete r;
}


using namespace HadoopPipes;
using namespace HadoopUtils;


struct test_factory {
  Factory* fp_;
  test_factory(Factory* fp): fp_(fp) {}
  RecordReader* createRecordReader(MapContext& ctx) {
    return fp_->createRecordReader(ctx);
  }
  Mapper* createMapper(MapContext& ctx) {
    std::cerr << "test_factory:: Ready to evaluate createMapper" << std::endl;
    Mapper* m = fp_->createMapper(ctx);
    std::cerr << "test_factory:: done." << std::endl;
    return m;
  }
  Reducer* createReducer(ReduceContext& ctx) {
    return fp_->createReducer(ctx);
  }
};


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


class TaskContextImpl: public TaskContext {
private:
  JobConfImpl job_conf;
  std::string input_key;
  std::string input_value;
  std::vector<int> counter_vals;
public:
  TaskContextImpl(const std::string& ik, const std::string& iv) :
    job_conf(), input_key(ik), input_value(iv) {}
  virtual const JobConf* getJobConf() { return &job_conf; }
  virtual const std::string& getInputKey() { return input_key; }
  virtual const std::string& getInputValue() { return input_value; }
  virtual void  emit(const std::string& k, const std::string& v) {
    std::cerr << "TaskContextImpl::emit("<<k<<", "<<v<<")"<<std::endl;
  }
  virtual void  progress() {
    std::cerr << "TaskContextImpl::progress()"<<std::endl;
  }
  virtual void  setStatus(const std::string& status) {
    std::cerr << "TaskContextImpl::setStatus("<<status<<")"<<std::endl;
  }
  virtual Counter* getCounter(const std::string& group,
			      const std::string& name) {
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


class ReduceContextImpl: public TaskContextImpl, public ReduceContext {
private:
  JobConfImpl job_conf;
  std::string input_key;
  std::string input_value;
  std::vector<int> counter_vals;
public:
  ReduceContextImpl(const std::string& ik, const std::string& iv) :
    TaskContextImpl(ik, iv), input_key(ik), input_value(iv) {}
  virtual bool nextValue() { return true; }
  virtual const JobConf* getJobConf() { return &job_conf; }
  virtual const std::string& getInputKey() { return input_key; }
  virtual const std::string& getInputValue() { return input_value; }
  virtual void  emit(const std::string& k, const std::string& v) {
    std::cerr << "TaskContextImpl::emit("<<k<<", "<<v<<")"<<std::endl;
  }
  virtual void  progress() {
    std::cerr << "ReduceContextImpl::progress()"<<std::endl;
  }
  virtual void  setStatus(const std::string& status) {
    std::cerr << "ReduceContextImpl::setStatus("<<status<<")"<<std::endl;
  }
  virtual Counter* getCounter(const std::string& group,
			      const std::string& name) {
    int id = counter_vals.size();
    counter_vals.push_back(0);
    return new Counter(id);
  }
  virtual void incrementCounter(const Counter* counter, uint64_t amount) {
    int id = counter->getId();
    counter_vals[id] += amount;
    std::cerr << "ReduceContextImpl::incrementCounter("<<id<<", "
	      <<amount<<")->"<< counter_vals[id]<< std::endl;
  }
};


class MapContextImpl: public TaskContextImpl, public MapContext {
private:
  const JobConf& job_conf;
  std::string input_key;
  std::string input_value;
  std::vector<int> counter_vals;
  std::string input_split;
  std::string input_key_class;
  std::string input_value_class;
public:
  MapContextImpl(const std::string& ik, const std::string& iv, 
		 const std::string& is, const std::string& ikc, 
		 const std::string& ivc, const JobConf& jc) :
    TaskContextImpl(ik, iv),
    job_conf(jc), input_key(ik), input_value(iv),
    input_split(is), input_key_class(ikc), input_value_class(ivc) {
  }
  virtual const JobConf* getJobConf() { return &job_conf; }
  virtual const std::string& getInputKey() { return input_key; }
  virtual const std::string& getInputValue() { return input_value; }
  virtual void  emit(const std::string& k, const std::string& v) {
    std::cerr << "MapContextImpl::emit("<<k<<", "<<v<<")"<<std::endl;
  }
  virtual void  progress() {
    std::cerr << "MapContextImpl::progress()"<<std::endl;
  }
  virtual void  setStatus(const std::string& status) {
    std::cerr << "MapContextImpl::setStatus("<<status<<")"<<std::endl;
  }
  virtual Counter* getCounter(const std::string& group,
			      const std::string& name) {
    int id = counter_vals.size();
    counter_vals.push_back(0);
    return new Counter(id);
  }
  virtual void incrementCounter(const Counter* counter, uint64_t amount) {
    int id = counter->getId();
    counter_vals[id] += amount;
    std::cerr << "MapContextImpl::incrementCounter("<<id<<", "
	      <<amount<<")->"<< counter_vals[id]<< std::endl;
  }
  virtual const std::string& getInputSplit() {
    return input_split;
  }
  virtual const std::string& getInputKeyClass() {
    return input_key_class;
  }
  virtual const std::string& getInputValueClass() {
    return input_value_class;
  }
};


JobConf* wrap_JobConf_object(JobConf& jc) { return &jc; }
JobConf* get_JobConf_object(bp::dict d) {
  JobConfImpl* jc = new JobConfImpl();
  bp::list keylist = d.keys();
  int  keylist_len = bp::extract<int>(keylist.attr("__len__")());
  for(int i = 0; i < keylist_len; ++i) {
    std::string k = bp::extract<std::string>(keylist[i]);
    std::string v = bp::extract<std::string>(d[keylist[i]]);
    jc->set(k, v);
  }
  return jc;
}

TaskContext* get_TaskContext_object(bp::dict d) {
  std::string ik = bp::extract<std::string>(d["input_key"]);
  std::string iv = bp::extract<std::string>(d["input_value"]);
  TaskContext* tc = new TaskContextImpl(ik, iv);
  return tc;
}

MapContext* get_MapContext_object(bp::dict d) {
  std::string ik = bp::extract<std::string>(d["input_key"]);
  std::string iv = bp::extract<std::string>(d["input_value"]);
  std::string is = bp::extract<std::string>(d["input_split"]);
  std::string ikc = bp::extract<std::string>(d["input_key_class"]);
  std::string ivc = bp::extract<std::string>(d["input_value_class"]);
  JobConf* jc = get_JobConf_object(bp::dict(d["job_conf"]));
  MapContext* mc = new MapContextImpl(ik, iv, is, ikc, ivc, *jc);
  return mc;
}

ReduceContext* get_ReduceContext_object(bp::dict d) {
  std::string ik = bp::extract<std::string>(d["input_key"]);
  std::string iv = bp::extract<std::string>(d["input_value"]);
  ReduceContext* rc = new ReduceContextImpl(ik, iv);
  return rc;
}


#endif // HADOOP_PIPES_TEST_SUPPORT_HPP
