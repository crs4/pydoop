import unittest
import random

import sys

#----------------------------------------------------------------------------
import hadoop_pipes

#----------------------------------------------------------------------------
class taskcontext_tc(unittest.TestCase):
  def setUp(self):
    pass
  #--
  def test_task_from_cpluplus(self):
    d = {'input_key' : 'inputkey',
         'input_value' : 'inputvalue'}
    o = hadoop_pipes.get_TaskContext_object(d)
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
  #--
  def test_mapcontext_from_cpluplus(self):
    d = {'input_key' : 'inputkey',
         'input_value' : 'inputvalue',
         'input_split' : 'inputsplit',
         'input_key_class' : 'keyclass',
         'input_value_class' : 'valueclass'}
    o = hadoop_pipes.get_MapContext_object(d)
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
  #--
  def test_reducecontext_from_cpluplus(self):
    d = {'input_key' : 'inputkey',
         'input_value' : 'inputvalue'}
    o = hadoop_pipes.get_ReduceContext_object(d)
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

#----------------------------------------------------------------------------
def suite():
  suite = unittest.TestSuite()
  #--
  suite.addTest(taskcontext_tc('test_task_from_cpluplus'))
  suite.addTest(taskcontext_tc('test_mapcontext_from_cpluplus'))
  suite.addTest(taskcontext_tc('test_reducecontext_from_cpluplus'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

