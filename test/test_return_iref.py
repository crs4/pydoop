import unittest

class TestCaseBoostPython(unittest.TestCase):
    def test_OuterInner(self):
        import test_iref
        import gc
        def do_test_value():
            outerTest = test_iref.Outer()
            inner = outerTest.innerval
            del outerTest
            return inner
            return inner
        def do_test():
            outerTest = test_iref.Outer()
            inner = outerTest.inner
            del outerTest
            return inner
        #This works but wrong lifetime
        innerTestVal = do_test_value()
        gc.collect()
        self.assertEqual(type(innerTestVal).__name__, 'Inner')
        #This also works
        innerTest = do_test()
        gc.collect()
        self.assertEqual(type(innerTest).__name__, 'Inner')

if __name__ == "__main__":
    from test.test_support import run_unittest
    run_unittest(TestCaseBoostPython)
