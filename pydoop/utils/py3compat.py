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


def __identity(x):
    return x


def __chr(x):
    return chr(x)


def __iteritems_2(x):
    return x.iteritems()


def __iteritems_3(x):
    return x.items()


if _is_py3:
    clong = int
    #  something that should be interpreted as a string
    basestring = str
    unicode = str
    xchr = __identity
    czip = zip
    cmap = map
    cfilter = filter
    iteritems = __iteritems_3
else:
    #  something that should be interpreted as bytes
    clong = long
    xchr = __chr
    iteritems = __iteritems_2
    from itertools import izip as czip
    from itertools import imap as cmap
    from itertools import ifilter as cfilter
