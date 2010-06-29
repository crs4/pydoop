# BEGIN_COPYRIGHT
# END_COPYRIGHT
import sys, unittest
import pydoop._pipes
from pydoop.pipes import Factory, Mapper, Reducer, runTask
from pydoop.pipes import RecordReader, RecordWriter


class mapper(Mapper):

  call_history=[]
  instance_counter = 0

  def __init__(self, ctx):
    Mapper.__init__(self)
    mapper.call_history.append('initialized')
    self.id = mapper.instance_counter
    mapper.instance_counter += 1

  def map(self, ctx):
    mapper.call_history.append('map() invoked')

  def __del__(self):
    sys.stderr.write("mapper.__del__ %d\n" % self.id)


class reducer(Reducer):

  call_history=[]
  instance_counter = 0

  def __init__(self, ctx):
    Reducer.__init__(self)
    reducer.call_history.append('initialized')
    self.id = reducer.instance_counter
    reducer.instance_counter += 1

  def reduce(self, ctx):
    reducer.call_history.append('reduce() invoked')

  def __del__(self):
    sys.stderr.write("reducer.__del__ %d\n" % self.id)


class record_reader(RecordReader):

  def __init__(self, ctx):
    RecordReader.__init__(self)
    self.ctx = ctx
    self.counter = 0

  def next(self):
    if self.counter < self.NUMBER_RECORDS:
      self.counter += 1
      return (True, self.KEY_FORMAT % self.counter, self.DEFAULT_VALUE)
    else:
      return (False, '', '')

  def getProgress(self):
    return float(self.counter)/self.NUMBER_RECORDS


class factory_tc(unittest.TestCase):

  def setUp(self):
    self.d = {'input_key' : 'inputkey',
              'input_value' : 'inputvalue',
              'input_split' : 'inputsplit',
              'input_key_class' : 'keyclass',
              'input_value_class' : 'valueclass',
              'job_conf' : {}}
    self.m_ctx = pydoop._pipes.get_MapContext_object(self.d)
    self.r_ctx = pydoop._pipes.get_ReduceContext_object(self.d)

  def __check_ctx(self):
    self.assertEqual(self.m_ctx.getInputKey(), self.d['input_key'])
    self.assertEqual(self.m_ctx.getInputValue(), self.d['input_value'])
    self.assertEqual(self.m_ctx.getInputSplit(), self.d['input_split'])
    self.assertEqual(self.m_ctx.getInputKeyClass(), self.d['input_key_class'])
    self.assertEqual(self.m_ctx.getInputValueClass(),
                     self.d['input_value_class'])

  def test_factory_costructor(self):
    f = Factory(mapper, reducer)
    self.failUnless(isinstance(f.createMapper(self.m_ctx), mapper))
    self.failUnless(isinstance(f.createReducer(self.r_ctx), reducer))
    #--
    f = Factory(mapper, reducer, record_reader)
    self.failUnless(isinstance(f.createMapper(self.m_ctx), mapper))
    self.failUnless(isinstance(f.createReducer(self.r_ctx), reducer))
    self.failUnless(isinstance(f.createRecordReader(self.m_ctx), record_reader))

  def test_map_reduce_factory(self):
    import gc
    self.__check_ctx()
    mapper.call_history = []
    reducer.call_history = []
    mf = Factory(mapper, reducer)
    gc.collect()  # clean up existing references
    pydoop._pipes.try_factory_internal(mf)
    self.assertEqual(0, gc.collect())
    self.assertEqual(len(mapper.call_history), 2)
    self.assertEqual(len(reducer.call_history), 2)
    f = pydoop._pipes.TestFactory(mf)
    self.failUnless(isinstance(f.createMapper(self.m_ctx), mapper))
    self.failUnless(isinstance(f.createReducer(self.r_ctx), reducer))
    self.assertEqual(len(mapper.call_history), 3)
    self.assertEqual(len(reducer.call_history), 3)
    self.assertEqual(0, gc.collect())


def suite():
  suite = unittest.TestSuite()
  suite.addTest(factory_tc('test_factory_costructor'))
  suite.addTest(factory_tc('test_map_reduce_factory'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
