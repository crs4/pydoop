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

from avro.io import DatumReader
from avro.datafile import DataFileReader


def main(fn, out_fn, avro_mode=''):
    with open(out_fn, 'w') as fo:
        with open(fn, 'rb') as f:
            reader = DataFileReader(f, DatumReader())
            for r in reader:
                if avro_mode.upper() == 'KV':
                    r = r['key']

                fo.write('%s\t%r\n' % (r['office'], r['counts']))
    print('wrote', out_fn)


if __name__ == '__main__':
    main(*sys.argv[1:])
