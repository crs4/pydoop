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
import sys
_is_py3 = sys.version_info[0] == 3

if _is_py3:
    class clong(int):
        pass
    class unicode(str):
        pass
    def xchr(x):
        return x
    basestring = str
else:
    class clong(long):
        pass
    def xchr(x):
        return chr(x)

