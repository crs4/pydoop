# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, subprocess, unittest


class TestIPCount(unittest.TestCase):

  def runTest(self):
    args = ["./ipcount", "-e", "excludes.txt", "input"]
    expected_out = [
      ["156.148.66.36", "682"],
      ["156.148.71.251", "566"],
      ["156.148.71.159", "154"],
      ["156.148.71.202", "153"],
      ["156.148.68.115", "122"],
      ]
    sys.stderr.write("\n")
    sys.stderr.flush()
    p = subprocess.Popen(args, stdout=subprocess.PIPE)
    out = p.communicate()[0].splitlines()
    out = [l.split() for l in out]
    self.assertEqual(out, expected_out)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestIPCount("runTest"))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
