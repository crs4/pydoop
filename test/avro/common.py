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

from pydoop.utils.py3compat import StringIO

from avro.io import DatumWriter, BinaryEncoder


class AvroSerializer(object):

    def __init__(self, schema):
        self.schema = schema
        self.datum_writer = DatumWriter(schema)

    def serialize(self, record):
        f = StringIO()
        encoder = BinaryEncoder(f)
        self.datum_writer.write(record, encoder)
        return f.getvalue()


def avro_user_record(i):
    return {
        "office": 'office-%s' % i,
        "favorite_number": i,
        "favorite_color": 'color-%s' % i,
        "name": 'name-%s' % i,
    }
