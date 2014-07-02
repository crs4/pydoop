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

#ifndef HADOOP_PIPES_CONTEXT_HPP
#define HADOOP_PIPES_CONTEXT_HPP


#include <string>
#include <vector>

#include <stdint.h>
#include <hadoop/Pipes.hh>

#ifdef __APPLE__
#include "mac_support.hpp"
#endif

#include <boost/python.hpp>


namespace bp = boost::python;
namespace hp = HadoopPipes;

#define GET_FROM_IMPL(cls, otype, method_name)	   \
  cls* o = this;                            \
  otype r = o->method_name();                      \
  return r;                          

#define GET_FROM_IMPL_1(cls, otype, method_name, a)\
  cls* o = this;                            \
  otype r = o->method_name(a);                     \
  return r;                          

#define GET_FROM_IMPL_2(cls, otype, method_name, a0, a1)	\
  cls* o = this;                            \
  otype r = o->method_name(a0, a1);	    \
  return r;                          

#define CALL_IMPL(cls, method_name) \
  cls* o = this;               \
  o->method_name();                


#define CALL_IMPL_1(cls, method_name, a) \
  cls* o = this;                    \
  o->method_name(a);                

#define CALL_IMPL_2(cls, method_name, a0, a1) \
  cls* o = this;                         \
  o->method_name(a0, a1);                



//+++++++++++++++++++++++++++++++++++++++++//
//                JobConf                  //
//+++++++++++++++++++++++++++++++++++++++++//

struct wrap_job_conf : hp::JobConf, bp::wrapper<hp::JobConf> {
  bool hasKey(const std::string& k) const {
    GET_FROM_IMPL_1(const hp::JobConf, bool, hasKey, k);
  }
  const std::string& get(const std::string& k) const {
    GET_FROM_IMPL_1(const hp::JobConf, const std::string&, get, k);
  }
  int getInt(const std::string& k) const {
    GET_FROM_IMPL_1(const hp::JobConf, int, getInt, k);
  }
  float getFloat(const std::string& k) const {
    GET_FROM_IMPL_1(const hp::JobConf, float, getFloat, k);
  }
  bool getBoolean(const std::string& k) const {
    GET_FROM_IMPL_1(const hp::JobConf, bool, getBoolean, k);
  }
};


//+++++++++++++++++++++++++++++++++++++++++//
//              TaskContext                //
//+++++++++++++++++++++++++++++++++++++++++//

struct wrap_task_context: hp::TaskContext, bp::wrapper<hp::TaskContext> {
  const hp::JobConf* getJobConf() {
    GET_FROM_IMPL(hp::TaskContext, const hp::JobConf*, getJobConf);
  }

  const std::string& getInputKey()   {
    GET_FROM_IMPL(hp::TaskContext, const std::string&, getInputKey);
  };
  const std::string& getInputValue() {
    GET_FROM_IMPL(hp::TaskContext, const std::string&, getInputValue);
  };
  void  emit(const std::string& k, const std::string& v) {
    CALL_IMPL_2(hp::TaskContext, emit, k, v);
  };
  void  progress() {
    CALL_IMPL(hp::TaskContext, progress);
  };
  void  setStatus(const std::string& status){
    CALL_IMPL_1(hp::TaskContext, setStatus, status);
  };
  Counter* getCounter(const std::string& group, const std::string& name) {
    GET_FROM_IMPL_2(hp::TaskContext, Counter*, getCounter, group, name);
  };
  void incrementCounter(const Counter* counter, uint64_t amount) { 
    CALL_IMPL_2(hp::TaskContext, incrementCounter, counter, amount);
  };
};


//+++++++++++++++++++++++++++++++++++++++++//
//              MapContext                 //
//+++++++++++++++++++++++++++++++++++++++++//

struct wrap_map_context: hp::MapContext, bp::wrapper<hp::MapContext> {
  const hp::JobConf* getJobConf() {
    GET_FROM_IMPL(hp::MapContext, const hp::JobConf*, getJobConf);
  }
  const std::string& getInputKey()   {
    GET_FROM_IMPL(hp::MapContext, const std::string&, getInputKey);
  };
  const std::string& getInputValue() {
    GET_FROM_IMPL(hp::MapContext, const std::string&, getInputValue);
  };
  void  emit(const std::string& k, const std::string& v) {
    CALL_IMPL_2(hp::MapContext, emit, k, v);
  };
  void  progress() {
    CALL_IMPL(hp::MapContext, progress);
  };
  void  setStatus(const std::string& status){
    CALL_IMPL_1(hp::MapContext, setStatus, status);
  };
  Counter* getCounter(const std::string& group, const std::string& name) {
    GET_FROM_IMPL_2(hp::MapContext, Counter*, getCounter, group, name);
  };
  void incrementCounter(const Counter* counter, uint64_t amount) { 
    CALL_IMPL_2(hp::MapContext, incrementCounter, counter, amount);
  };
  //-------------------------
  const std::string& getInputSplit() {
    GET_FROM_IMPL(hp::MapContext, const std::string&, getInputSplit);
  }
  const std::string& getInputKeyClass() {
    GET_FROM_IMPL(hp::MapContext, const std::string&, getInputKeyClass);
  }
  const std::string& getInputValueClass() {
    GET_FROM_IMPL(hp::MapContext, const std::string&, getInputValueClass);
  }
};

//+++++++++++++++++++++++++++++++++++++++++//
//              ReduceContext              //
//+++++++++++++++++++++++++++++++++++++++++//

struct wrap_reduce_context: hp::ReduceContext, bp::wrapper<hp::ReduceContext> {
  const hp::JobConf* getJobConf() {
    GET_FROM_IMPL(hp::ReduceContext, const hp::JobConf*, getJobConf);
  }
  const std::string& getInputKey()   { 
    GET_FROM_IMPL(hp::ReduceContext, const std::string&, getInputKey);
  };
  const std::string& getInputValue() {
    GET_FROM_IMPL(hp::ReduceContext, const std::string&, getInputValue);
  };
  void  emit(const std::string& k, const std::string& v) {
    CALL_IMPL_2(hp::ReduceContext, emit, k, v);
  };
  void  progress() {
    CALL_IMPL(hp::ReduceContext, progress);
  };
  void  setStatus(const std::string& status){
    CALL_IMPL_1(hp::ReduceContext, setStatus, status);
  };
  Counter* getCounter(const std::string& group, const std::string& name) {
    GET_FROM_IMPL_2(hp::ReduceContext, Counter*, getCounter, group, name);
  };
  void incrementCounter(const Counter* counter, uint64_t amount) { 
    CALL_IMPL_2(hp::ReduceContext, incrementCounter, counter, amount);
  };
  //-------------------------
  bool nextValue() {
    GET_FROM_IMPL(hp::ReduceContext, bool, nextValue);
  }
};

#endif // HADOOP_PIPES_CONTEXT_HPP
