# BEGIN_COPYRIGHT
# END_COPYRIGHT
import unittest, imp


TEST_MODULE_NAMES = [
  "all_tests_pipes",
  "all_tests_hdfs"
  ]


def suite():
  suites = []
  for name in TEST_MODULE_NAMES:
    fp, pathname, description = imp.find_module(name)
    try:
      module = imp.load_module(name, fp, pathname, description)
      suites.append(module.suite())
    finally:
      fp.close()
  return unittest.TestSuite(tuple(suites))


if __name__ == '__main__':
  unittest.TextTestRunner(verbosity=2).run(suite())
