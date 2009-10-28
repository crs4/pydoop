# BEGIN_COPYRIGHT
# END_COPYRIGHT
import unittest
import pydoop_pipes
from pydoop.pipes import InputSplit


example_input_splits = [
  ('\x00/hdfs://localhost:9000/user/zag/in-dir/FGCS-1.ps\x00\x00\x00\x00\x00\x08h(\x00\x00\x00\x00\x00\x08h\x05',
   'hdfs://localhost:9000/user/zag/in-dir/FGCS-1.ps',
   550952, 550917),
  ('\x00/hdfs://localhost:9000/user/zag/in-dir/FGCS-1.ps\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08h(',
   'hdfs://localhost:9000/user/zag/in-dir/FGCS-1.ps',
   0, 550952),
  ('\x001hdfs://localhost:9000/user/zag/in-dir/images_list\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00$',
   'hdfs://localhost:9000/user/zag/in-dir/images_list',
   0, 36)
  ]


class taskcontext_tc(unittest.TestCase):
  
  def setUp(self):
    pass

  def test_task_from_cpluplus(self):
    d = {'input_key' : 'inputkey',
         'input_value' : 'inputvalue'}
    o = pydoop_pipes.get_TaskContext_object(d)
    self.assertEqual(o.getInputKey(), d['input_key'])
    self.assertEqual(o.getInputValue(), d['input_value'])
    jc = o.getJobConf()
    self.assertFalse(jc.hasKey('nononono'))
    c = o.getCounter('hello', 'there')
    o.incrementCounter(c, 29292)
    o.incrementCounter(c, 29292)
    o.progress()
    o.setStatus('hello')
    o.emit('key', 'vall')

  def test_mapcontext_from_cpluplus(self):
    d = {'input_key' : 'inputkey',
         'input_value' : 'inputvalue',
         'input_split' : 'inputsplit',
         'input_key_class' : 'keyclass',
         'input_value_class' : 'valueclass'}
    o = pydoop_pipes.get_MapContext_object(d)
    self.assertEqual(o.getInputKey(), d['input_key'])
    self.assertEqual(o.getInputValue(), d['input_value'])
    self.assertEqual(o.getInputSplit(), d['input_split'])
    self.assertEqual(o.getInputKeyClass(), d['input_key_class'])
    self.assertEqual(o.getInputValueClass(), d['input_value_class'])
    jc = o.getJobConf()
    self.assertFalse(jc.hasKey('nononono'))
    c = o.getCounter('hello', 'there')
    o.incrementCounter(c, 29292)
    o.incrementCounter(c, 29292)
    o.progress()
    o.setStatus('hello')
    o.emit('key', 'vall')

  def test_reducecontext_from_cpluplus(self):
    d = {'input_key' : 'inputkey',
         'input_value' : 'inputvalue'}
    o = pydoop_pipes.get_ReduceContext_object(d)
    self.assertEqual(o.getInputKey(), d['input_key'])
    self.assertEqual(o.getInputValue(), d['input_value'])
    jc = o.getJobConf()
    self.assertFalse(jc.hasKey('nononono'))
    c = o.getCounter('hello', 'there')
    o.incrementCounter(c, 29292)
    o.incrementCounter(c, 29292)
    o.progress()
    o.setStatus('hello')
    o.emit('key', 'vall')

  def test_input_split(self):
    for s in example_input_splits:
      i = InputSplit(s[0])
      self.assertEqual(i.filename, s[1])
      self.assertEqual(i.offset, s[2])
      self.assertEqual(i.length, s[3])


def suite():
  suite = unittest.TestSuite()
  suite.addTest(taskcontext_tc('test_task_from_cpluplus'))
  suite.addTest(taskcontext_tc('test_mapcontext_from_cpluplus'))
  suite.addTest(taskcontext_tc('test_reducecontext_from_cpluplus'))
  suite.addTest(taskcontext_tc('test_input_split'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
