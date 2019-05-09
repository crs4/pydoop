// BEGIN_COPYRIGHT
//
// Copyright 2009-2019 CRS4.
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

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"

# define INT64_SIZE sizeof(int64_t)


int64_t deserializeLongWritable(std::string s) {
  int64_t rval = 0;
  if (s.size() < INT64_SIZE) {
    throw std::invalid_argument("not enough bytes");
  }
  for (std::size_t i = 0; i < INT64_SIZE; ++i) {
    rval = (rval << INT64_SIZE) | static_cast<unsigned char>(s[i]);
  }
  return rval;
}


class Mapper: public HadoopPipes::Mapper {

public:
  Mapper(HadoopPipes::TaskContext &context) { }

  void map(HadoopPipes::MapContext &context) {
    int64_t key = deserializeLongWritable(context.getInputKey());
    std::cerr << "key (ignored): " << key << "\n";
    std::stringstream ss(context.getInputValue());
    std::string item;
    while (std::getline(ss, item, ' ')) {
      context.emit(item, "1");
    }
  }

};


class Reducer: public HadoopPipes::Reducer {

public:
  Reducer(HadoopPipes::TaskContext &context) { }

  void reduce(HadoopPipes::ReduceContext &context) {
    int sum = 0;
    while (context.nextValue()) {
      sum += HadoopUtils::toInt(context.getInputValue());
    }
    context.emit(context.getInputKey(), HadoopUtils::toString(sum));
  }
};


int main(int argc, char *argv[]) {
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<Mapper, Reducer>());
}
