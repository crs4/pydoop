# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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
import os
import importlib


_TEST_DIRS = (
    "app",
    "hdfs",
    "mapreduce",
    "serialize",
    "common",
)


def suite():
    suites = []
    for dir_ in _TEST_DIRS:
        module = importlib.import_module("%s.%s" % (dir_, "all_tests"))
        sys.path.insert(0, dir_)
        path = [os.path.abspath("./%s" % dir_)]
        suites.append(getattr(module, "suite")(path))
        sys.path.pop(0)
    return unittest.TestSuite(tuple(suites))


if __name__ == '__main__':
    import sys
    _RESULT = unittest.TextTestRunner(verbosity=2).run(suite())
    sys.exit(not _RESULT.wasSuccessful())
