# BEGIN_COPYRIGHT
# END_COPYRIGHT
import unittest


import pydoop_pipes
from pydoop.pipes import Factory, Partitioner


def partition_function(k, n):
  return len(k) % n


class test_partitioner(Partitioner):

  def __init__(self, ctx):
    Partitioner.__init__(self)
    self.ctx = ctx
    self.counter = 0

  def partition(self, key, num_of_reduces):
    return partition_function(key, num_of_reduces)


class partitioner_tc(unittest.TestCase):

  def setUp(self):
    pass

  def test_partitioner_from_cpluplus(self):
    d = {'input_key' : 'inputkey',
         'input_value' : 'inputvalue',
         'input_split' : 'inputsplit',
         'input_key_class' : 'keyclass',
         'input_value_class' : 'valueclass',
         'job_conf' : {}
         }
    ctx = pydoop_pipes.get_MapContext_object(d)
    self.assertEqual(ctx.getInputKey(), d['input_key'])
    self.assertEqual(ctx.getInputValue(), d['input_value'])
    self.assertEqual(ctx.getInputSplit(), d['input_split'])
    self.assertEqual(ctx.getInputKeyClass(), d['input_key_class'])
    self.assertEqual(ctx.getInputValueClass(), d['input_value_class'])
    f = Factory(None, None, partitioner_class=test_partitioner)
    p = f.createPartitioner(ctx)
    n_partitions = 4
    for i in range(10):
      k = 'key' + ('a' * i)
      self.assertEqual(
        partition_function(k, n_partitions),
        pydoop_pipes.get_partition_from_partitioner(p, k, n_partitions)
        )


def suite():
  suite = unittest.TestSuite()
  suite.addTest(partitioner_tc('test_partitioner_from_cpluplus'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
