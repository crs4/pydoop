from pydoop import Mapper, Reducer, Factory, runTask


mfl = open('/tmp/wordcount_mapper.log', 'w')
def mlog(x):
  mfl.write('%s\n' % x)
  mfl.flush()



WORDCOUNT    = 'WORDCOUNT'
INPUT_WORDS  = 'INPUT_WORDS'
OUTPUT_WORDS = 'OUTPUT_WORDS'

class WC_Mapper(Mapper):
  def __init__(self, task_ctx):
    mlog('WC_mapper::init with task=%s' % dir(task_ctx))
    self.inputWords = task_ctx.getCounter(WORDCOUNT, INPUT_WORDS)
    mlog('WC_mapper::init self.inputWords=%s' % self.inputWords)
  #-
  def map(self, map_ctx):
    mlog('WC_mapper::map on task=%s' % dir(task_ctx))
    words = map_ctx.getInputValue().split()
    for w in words:
      map_ctx.emit(w, '1')
    map_ctx.incrementCounter(self.inputWords, len(words))

class WC_Reducer(Reducer):
  def __init__(self, task_ctx):
    self.outputWords = task_ctx.getCounter(WORDCOUNT, OUTPUT_WORDS)
  #-
  def reduce(self, red_ctx):
    s = 0
    while red_ctx.nextValue():
      s += int(red_ctx.getInputValue())
    red_ctx.emit(red_ctx.getInputKey(), str(s))
    red_ctx.incrementCounter(self.outputWords, 1)

runTask(Factory(WC_Mapper, WC_Reducer))
