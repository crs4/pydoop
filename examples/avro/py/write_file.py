# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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

import random

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


def main():
    schema = avro.schema.parse(open("../schemas/user.avsc").read())
    writer = DataFileWriter(open("users.avro", "w"), DatumWriter(), schema)

    N = 200
    offices = ['office-%s' % i for i in xrange(3)]
    colors = ['red', 'blue', 'yellow', 'orange', 'maroon', 'green']
    names = ['Alyssa', 'John', 'Kathy', 'Ben', 'Karla', 'Ross', 'Violetta']

    for i in xrange(N):
        writer.append({
            "office": random.choice(offices),
            "favorite_number": random.choice(range(10)),
            "favorite_color":  random.choice(colors),
            "name": random.choice(names),
        })
    writer.close()


if __name__ == '__main__':
    main()
