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
import random

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

if sys.version_info[0] == 3:
    xrange = range
    parse = avro.schema.Parse
else:
    parse = avro.schema.parse

NAME_POOL = ['george', 'john', 'paul', 'ringo']
OFFICE_POOL = ['office-%d' % _ for _ in xrange(4)]
COLOR_POOL = ['black', 'cyan', 'magenta', 'yellow']


def main(argv):
    try:
        schema_fn = argv[1]
        n_users = int(argv[2])
        avro_fn = argv[3]
    except IndexError:
        sys.exit('Usage: %s SCHEMA_FILE N_USERS AVRO_FILE' % argv[0])
    with open(schema_fn) as f_in:
        schema = parse(f_in.read())
    with open(avro_fn, 'wb') as f_out:
        writer = DataFileWriter(f_out, DatumWriter(), schema)
        for i in xrange(n_users):
            writer.append({
                'name': random.choice(NAME_POOL),
                'office': random.choice(OFFICE_POOL),
                'favorite_color': random.choice(COLOR_POOL),
                'favorite_number': i,
            })
        writer.close()


if __name__ == '__main__':
    main(sys.argv)
