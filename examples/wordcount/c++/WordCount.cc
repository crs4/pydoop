/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Based upon the Hadoop C++ examples
 * Modified by CRS4
 */

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"
#include <stdlib.h>

class Mapper: public HadoopPipes::Mapper {
private:
  HadoopPipes::TaskContext *context;
public:
  Mapper(HadoopPipes::TaskContext& context) {
    this->context = &context;
  }
  void map(HadoopPipes::MapContext& context) {
    std::vector<std::string> words =
      HadoopUtils::splitString(context.getInputValue(), " ");
    for(unsigned int i=0; i < words.size(); ++i) {
      context.emit(words[i], "1");
    }
  }
  void close() {
    // emit after seeing all tuples -- useful for accumulation
    this->context->emit("JUST_ONE_MORE", "0");
  }
};


class Reducer: public HadoopPipes::Reducer {
public:
  Reducer(HadoopPipes::TaskContext& context) {}
  void reduce(HadoopPipes::ReduceContext& context) {
    int sum = 0;
    while (context.nextValue()) {
      sum += HadoopUtils::toInt(context.getInputValue());
    }
    context.emit(context.getInputKey(), HadoopUtils::toString(sum));
  }
};


int main(int argc, char *argv[]) {
  char * hadoop_command_port;
	char * mapreduce_command_port;

  hadoop_command_port = getenv("hadoop.pipes.command.port");
	mapreduce_command_port = getenv("mapreduce.pipes.command.port");


  if(hadoop_command_port != NULL && mapreduce_command_port == NULL){
    setenv("mapreduce.pipes.command.port", hadoop_command_port, 1);

  }

  if(hadoop_command_port == NULL && mapreduce_command_port != NULL){
    setenv("hadoop.pipes.command.port", mapreduce_command_port, 1);

  }

  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<Mapper, Reducer>());
}
