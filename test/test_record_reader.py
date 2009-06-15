import unittest
import random

import sys

#----------------------------------------------------------------------------
import pydoop_core
from pydoop import Factory

class test_record_reader(pydoop_core.RecordReader):
  DEFAULT_VALUE='The quick red fox jumped on the lazy brown dog'
  KEY_FORMAT='key-%d'
  NUMBER_RECORDS=10
  def __init__(self, ctx):
    pydoop_core.RecordReader.__init__(self)
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

#----------------------------------------------------------------------------
class record_reader_tc(unittest.TestCase):
  def setUp(self):
    pass
  #--
  def test_record_reader_from_cpluplus(self):
    d = {'input_key' : 'inputkey',
         'input_value' : 'inputvalue',
         'input_split' : 'inputsplit',
         'input_key_class' : 'keyclass',
         'input_value_class' : 'valueclass'}
    ctx = pydoop_core.get_MapContext_object(d)
    self.assertEqual(ctx.getInputKey(), d['input_key'])
    self.assertEqual(ctx.getInputValue(), d['input_value'])
    self.assertEqual(ctx.getInputSplit(), d['input_split'])
    self.assertEqual(ctx.getInputKeyClass(), d['input_key_class'])
    self.assertEqual(ctx.getInputValueClass(), d['input_value_class'])

    f = Factory(None, None, test_record_reader)
    rr = f.createRecordReader(ctx)
    for i in range(test_record_reader.NUMBER_RECORDS):
      (f, k, v) = pydoop_core.get_record_from_record_reader(rr)
      self.assertTrue(f)
      self.assertEqual(k, test_record_reader.KEY_FORMAT % (i+1))
      self.assertEqual(v, test_record_reader.DEFAULT_VALUE)
      self.assertAlmostEqual(pydoop_core.get_progress_from_record_reader(rr),
                             float(i+1)/test_record_reader.NUMBER_RECORDS)
    (f, k, v) = pydoop_core.get_record_from_record_reader(rr)
    self.assertFalse(f)

#----------------------------------------------------------------------------
def suite():
  suite = unittest.TestSuite()
  #--
  suite.addTest(record_reader_tc('test_record_reader_from_cpluplus'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

