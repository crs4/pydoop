#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"


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
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<Mapper, Reducer>());
}
