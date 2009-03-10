#ifndef HADOOP_PIPES_HPP
#define HADOOP_PIPES_HPP


#include "hadoop_pipes_context.hpp"

using namespace HadoopPipes;

#include <string>

struct wrap_mapper: Mapper, wrapper<Mapper> {
  void map(MapContext& ctx) {
    this->get_override("map")(ctx);
  }
};

struct wrap_reducer: Reducer, wrapper<Reducer> {
  void reduce(ReduceContext& ctx) {
    this->get_override("reduce")(ctx);
  }
};

struct wrap_partitioner: Partitioner, wrapper<Partitioner> {
  int partition(const std::string& key, int numOfReduces) {
    return this->get_override("partition")(key, numOfReduces);
  }
};

struct wrap_record_reader: RecordReader, wrapper<RecordReader> {
  bool next(std::string& key, std::string& value) {
    return this->get_override("next")(key, value);
  }
  float getProgress() {
    return this->get_override("getProgress")();
  }
};

struct wrap_record_writer: RecordWriter, wrapper<RecordWriter> {
  void emit(const std::string& key, const std::string& value) {
    this->get_override("emit")(key, value);
  }
};

#define OVERRIDE_CREATOR_IF_POSSIBLE(base, ctx_type, method_name, arg)	\
  if (override f = this->get_override("method_name")) { \
    const ctx_type& c_ctx = ctx;\
    object o_ctx = make_getter(&c_ctx, \
			       return_value_policy<copy_const_reference>());\
    return f(o_ctx); \
  } else { \
    return base::method_name(arg); \
  }

struct wrap_factory: Factory, wrapper<Factory> {
  //----------------------------------------------------------
  Mapper* createMapper(MapContext& ctx) const {
#if 1    
    const MapContext& c_ctx = ctx;
    object o_ctx = make_getter(&c_ctx, 
			       return_value_policy<copy_const_reference>());
    return this->get_override("createMapper")(o_ctx);
#else
    std::cerr << "wrap_factory:: m=" << m << std::endl;
    
    std::cerr << "wrap_factory:: ctx=" << &(ctx) << std::endl;
    override f = ;
    std::cerr << "wrap_factory:: f="  << std::endl;
    std::cerr << "wrap_factory:: ctx.getInputKey()="  
	      << ctx.getInputKey() << std::endl;
    const MapContext& c_ctx = ctx;
    object o_ctx = make_getter(&c_ctx, 
			       return_value_policy<copy_const_reference>());
    Mapper* m = f(o_ctx);
    std::cerr << "wrap_factory:: m=" << m << std::endl;
    return m;
#endif
  }
  Reducer* createReducer(ReduceContext& ctx) const{
    const ReduceContext& c_ctx = ctx;
    object o_ctx = make_getter(&c_ctx, 
			       return_value_policy<copy_const_reference>());
    return this->get_override("createReducer")(o_ctx);
  }
  //----------------------------------------------------------
  Reducer* createCombiner(MapContext& ctx) const {
    OVERRIDE_CREATOR_IF_POSSIBLE(Factory, MapContext, 
				 createCombiner, ctx);
  }
  Reducer* default_create_combiner(MapContext& ctx) const {
    return this->Factory::createCombiner(ctx);
  }
  //----------------------------------------------------------
  Partitioner* createPartitioner(MapContext& ctx) const {
    OVERRIDE_CREATOR_IF_POSSIBLE(Factory, MapContext,
				 createPartitioner, ctx);
  }
  Partitioner* default_create_partitioner(MapContext& ctx) const {
    return this->Factory::createPartitioner(ctx);
  }
  //----------------------------------------------------------
  RecordWriter* createRecordWriter(ReduceContext& ctx) const {
    OVERRIDE_CREATOR_IF_POSSIBLE(Factory, ReduceContext,
				 createRecordWriter, ctx);
  }
  RecordWriter* default_create_record_writer(ReduceContext& ctx) const {
    return this->Factory::createRecordWriter(ctx);
  }
  //----------------------------------------------------------
  RecordReader* createRecordReader(MapContext& ctx) const {
    OVERRIDE_CREATOR_IF_POSSIBLE(Factory, MapContext,
				 createRecordReader, ctx);
  }
  RecordReader* default_create_record_reader(MapContext& ctx) const {
    return this->Factory::createRecordReader(ctx);
  }
};

#endif  // HADOOP_PIPES_HPP

