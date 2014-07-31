import unittest, os, tempfile, shutil


class WDTestCase(unittest.TestCase):

    def setUp(self):
        self.wd = tempfile.mkdtemp(prefix='pydoop_test_')

    def tearDown(self):
        shutil.rmtree(self.wd)

    def _mkfn(self, basename):
        return os.path.join(self.wd, basename)

    def _mkf(self, basename):
        return open(self._mkfn(basename), 'w')
