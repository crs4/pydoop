import unittest
from pydoop.utils.serialize import FlowWriter, FlowReader
from pydoop.test_utils import WDTestCase


class TestFlow(WDTestCase):
    def write_read_helper(self, N, rule, data):
        fname = 'foo.dat'  # self._mkfn('foo.dat')
        with FlowWriter(open(fname, 'wb')) as stream:
            for _ in range(N):
                stream.write(rule, data)
        with FlowReader(open(fname, 'rb')) as stream:
            for _ in range(N):
                xdata = stream.read(rule)
                self.assertEqual(len(xdata), len(data))
                for x, y in zip(xdata, data):
                    if isinstance(x, float):
                        self.assertAlmostEqual(x, y)
                    else:
                        self.assertEqual(x, y)

    def test_write_read_basic(self):
        rule = b'sfis'
        data = (b'A string', 0.33, 22233, b'Another String')
        N = 10
        self.write_read_helper(N, rule, data)

    def test_write_read_list_of_strings(self):
        rule = b'A'
        data = ((b'aaa', b'bbb', b'ccc', b'ddd'),)
        N = 10
        self.write_read_helper(N, rule, data)

    def test_write_read_mixed(self):
        rule = b'fAsiisS'
        data = (0.54, (b'aaa', b'bbb', b'ccc', b'ddd'),
                b'ssed', 4343, 35555, b'weweew', b'ooioioi')
        N = 10
        self.write_read_helper(N, rule, data)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestFlow)


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
