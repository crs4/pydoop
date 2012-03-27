# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest
import pydoop.hadut as hadut


def pair_set(seq):
  return set((seq[i], seq[i+1]) for i in xrange(0, len(seq), 2))


class TestHadut(unittest.TestCase):

  def assertEqualPairSet(self, seq1, seq2):
    return self.assertEqual(pair_set(seq1), pair_set(seq2))

  def test_pop_generic_args(self):
    self.assertRaises(ValueError, hadut._pop_generic_args, ['-fs'])
    args = [
      '-input', 'i',
      '-libjars', 'l',
      '-output', 'o',
      '-fs', 'f',
      '-jar', 'pippo'
      ]
    gargs = hadut._pop_generic_args(args)
    self.assertEqualPairSet(gargs, ['-libjars', 'l', '-fs', 'f'])
    self.assertEqualPairSet(
      args, ['-input', 'i', '-output', 'o', '-jar', 'pippo']
      )


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestHadut('test_pop_generic_args'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
