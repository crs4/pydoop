# -*- coding: utf-8 -*-
# vim: set fileencoding: utf-8

# BEGIN_COPYRIGHT
#
# Copyright 2009-2016 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import unittest

import pydoop.sercore as sc


class TestWritable(unittest.TestCase):

    def setUp(self):
        self.rules = sc.WritableRules()
        self.rules.add(int, ((b'', b'i'),))
        self.rules.add(float, ((b'', b'f'),))
        self.rules.add(bytes, ((b'', b's'),))

    def writable_writer_helper(self, values, compare=None):
        # FIXME: use tmp file
        fname = 'goo.bin'
        with open(fname, 'wb') as f:
            ww = sc.WritableWriter(f, self.rules)
            for x in values:
                ww.write(x)
            ww.flush()
        with open(fname, 'rb') as f:
            wr = sc.WritableReader(f, self.rules, None)
            for x in values:
                r = wr.read(type(x))
                if compare:
                    compare(r, x)
                else:
                    if (type(x) == float):
                        self.assertAlmostEqual(x, r)
                    else:
                        self.assertEqual(x, r)

    def test_exceptions(self):
        fname = 'goo.bin'
        with open(fname, 'wb') as f:
            ww = sc.WritableWriter(f, self.rules)
            c = complex()
            self.assertRaises(ValueError, ww.write, c)
        with open(fname, 'rb') as f:
            wr = sc.WritableReader(f, self.rules, None)
            self.assertRaises(ValueError, wr.read, None)
            self.assertRaises(ValueError, wr.read, complex)
        with open(fname, 'wb') as f:
            self.assertRaises(ValueError, sc.WritableReader,
                              f, self.rules, None)

    def test_wr_default(self):
        fname = 'goo.bin'
        values = range(1, 100)
        with open(fname, 'wb') as f:
            ww = sc.WritableWriter(f, self.rules)
            for x in values:
                ww.write(x)
            ww.flush()
        with open(fname, 'rb') as f:
            wr = sc.WritableReader(f, self.rules, int)
            for v in values:
                g = wr.read(None)
                self.assertEqual(g, v)

    def test_wr_eof(self):
        fname = 'goo.bin'
        with open(fname, 'wb') as f:
            pass
        with open(fname, 'rb') as f:
            wr = sc.WritableReader(f, self.rules, int)
            self.assertRaises(EOFError, wr.read, None)

    def test_wr_buffer(self):
        fname = 'goo.bin'
        values = range(1, 1000)
        with open(fname, 'wb') as f:
            ww = sc.WritableWriter(f, self.rules)
            for x in values:
                ww.write(x)
            ww.flush()
        with open(fname, 'rb') as f:
            data = f.read()
        wr = sc.WritableReader(data, self.rules, int)
        for v in values:
            g = wr.read(None)
            self.assertEqual(g, v)

    def test_intrinsics(self):
        values = [1, 10, 20, 849898989, 0.333, b'this here is bytes']
        self.writable_writer_helper(values)

    def test_user_defined_simple_types(self):
        class LongLong(int):
            pass
        self.rules.add(LongLong, ((b'', b'L'),))
        values = [LongLong(x) for x in [89289839898, 8938988989]]
        self.writable_writer_helper(values)

    def test_user_defined_complex_types(self):
        class Foo():
            def __init__(self):
                self.a = None
                self.b = None

        def foo_maker(a, b):
            f = Foo()
            f.a = a
            f.b = b
            return f

        def assert_equal(foo1, foo2):
            self.assertEqual(foo1.a, foo2.a)
            self.assertAlmostEqual(foo1.b, foo2.b)
        self.rules.add(Foo, ((b'a', b'i'), (b'b', b'f')))
        values = [foo_maker(x, y) for x, y in [(1, 0.22), (2, 0.33)]]
        self.writable_writer_helper(values, assert_equal)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestWritable)


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
