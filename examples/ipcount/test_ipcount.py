# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, subprocess, unittest, tempfile, shutil


class TestIPCount(unittest.TestCase):

  def runTest(self):
    temp_dir = tempfile.mkdtemp(prefix="pydoop_test_ipcount_")
    outfn = os.path.join(temp_dir, "ipcount.out")
    args = ["./ipcount", "-e", "excludes.txt", "input", "--local-output", outfn]
    expected_out = [
      ["156.148.66.36", "682"],
      ["156.148.71.251", "566"],
      ["156.148.71.159", "154"],
      ["156.148.71.202", "153"],
      ["156.148.68.115", "122"],
      ]
    sys.stderr.write("\n")
    sys.stderr.flush()
    p = subprocess.check_call(args)
    outf = open(outfn)
    out = [l.split() for l in outf]
    outf.close()
    self.assertEqual(out, expected_out)
    shutil.rmtree(temp_dir)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestIPCount("runTest"))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
