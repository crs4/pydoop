# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest
import pydoop.text_protocol as tp


PIPES_SCRIPT = "../examples/wordcount/bin/wordcount-minimal.py"


class TestTextDownProtocol(unittest.TestCase):

  def test_run_map(self):
    print
    p = tp.text_down_protocol(PIPES_SCRIPT)
    p.start()
    p.run_map("fake", 2)
    p.map_item("1", "a b a")
    p.close()

  def test_run_reduce(self):
    print
    p = tp.text_down_protocol(PIPES_SCRIPT)
    p.start()
    p.run_reduce()
    p.reduce_key("a")
    for _ in 1, 2:
      p.reduce_value("1")
    p.reduce_key("b")
    p.reduce_value("1")
    p.close()


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestTextDownProtocol('test_run_map'))
  suite.addTest(TestTextDownProtocol('test_run_reduce'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
