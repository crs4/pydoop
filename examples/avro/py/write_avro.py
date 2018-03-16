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
import csv

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

parse = avro.schema.Parse if sys.version_info[0] == 3 else avro.schema.parse
FIELDS = ['name', 'office', 'favorite_color']


def main(schema_fn, csv_fn, avro_fn):

    with open(schema_fn) as f_in:
        schema = parse(f_in.read())

    with open(csv_fn) as f_in:
        reader = csv.reader(f_in, delimiter=';')
        with open(avro_fn, 'wb') as f_out:
            writer = DataFileWriter(f_out, DatumWriter(), schema)
            for row in reader:
                writer.append(dict(zip(FIELDS, row)))
            writer.close()


if __name__ == '__main__':
    try:
        schema_fn = sys.argv[1]
        csv_fn = sys.argv[2]
        avro_fn = sys.argv[3]
    except IndexError:
        sys.exit('Usage: %s SCHEMA_FILE CSV_FILE AVRO_FILE' % sys.argv[0])
    main(schema_fn, csv_fn, avro_fn)
