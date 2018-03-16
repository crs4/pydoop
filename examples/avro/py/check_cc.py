# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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
import os
import errno
from collections import Counter

from avro.io import DatumReader
from avro.datafile import DataFileReader
from pydoop.utils.py3compat import iteritems


def iter_fnames(path):
    try:
        contents = os.listdir(path)
    except OSError as e:
        if e.errno == errno.ENOTDIR:
            yield path
    else:
        for name in contents:
            yield os.path.join(path, name)


def main(in_, out_):

    expected = {}
    for in_fn in iter_fnames(in_):
        with open(in_fn, 'rb') as f:
            reader = DataFileReader(f, DatumReader())
            for r in reader:
                expected.setdefault(
                    r["office"], Counter()
                )[r["favorite_color"]] += 1

    computed = {}
    for out_fn in iter_fnames(out_):
        with open(out_fn) as f:
            for l in f:
                p = l.strip().split('\t')
                computed[p[0]] = eval(p[1])

    if set(computed) != set(expected):
        sys.exit("ERROR: computed keys != expected keys: %r != %r" % (
            sorted(computed), sorted(expected)))
    for k, v in iteritems(expected):
        if computed[k] != v:
            sys.exit("ERROR: %r: %r != %r" % (k, computed[k], dict(v)))
    print('All is ok!')


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
