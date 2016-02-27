# BEGIN_COPYRIGHT
#
# Copyright 2009-2016 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import sys
from collections import Counter


def main(efname, rfname):

    expected = {}
    with open(efname) as f:
        for l in f:
            p = l.strip().split(';')
            expected.setdefault(p[1], Counter())[p[2]] += 1

    computed = {}
    with open(rfname) as f:
        for l in f:
            p = l.strip().split('\t')
            computed[p[0]] = eval(p[1])

    if set(computed) != set(expected):
        sys.exit("ERROR: computed keys != expected keys: %r != %r" % (
            sorted(computed), sorted(expected)))
    for k, v in expected.iteritems():
        if computed[k] != v:
            sys.exit("ERROR: %r: %r != %r" % (k, computed[k], dict(v)))
    print 'All is ok!'


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
