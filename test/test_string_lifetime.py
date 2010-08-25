# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest
import pydoop._pipes

import gc


class str_lifetime_tc(unittest.TestCase):

  def memory_stress(self):
    N = 10000
    S = 10000000
    for x in xrange(N):
      s = pydoop._pipes.create_a_string(S)

  def push_leak_map_context(self):
    N = 100
    S = 100000000
    big_string = 'a' * S
    d = {'input_key' : 'foo_key',
         'input_value' : big_string,
         'input_split' : '',
         'input_key_class' : 'foo_key_class',
         'input_value_class' : 'foo_value_class',
         'job_conf': {}}
    for x in xrange(N):
      mctx = pydoop._pipes.get_MapContext_object(d)
      v = mctx.getInputValue()

  def garbage_collect(self):
    N = 10000
    S = 10000000
    for x in xrange(N):
      s = pydoop._pipes.create_a_string(S)
      gc.collect()


def suite():
  suite = unittest.TestSuite()
  suite.addTest(str_lifetime_tc('push_leak_map_context'))
  #suite.addTest(str_lifetime_tc('garbage_collect'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
