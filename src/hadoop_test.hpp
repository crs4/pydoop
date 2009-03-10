#ifndef HADOOP_TEST_HPP
#define HADOOP_TEST_HPP

using namespace HadoopPipes;

#include <string>
#include <iostream>

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
#endif // HADOOP_TEST
