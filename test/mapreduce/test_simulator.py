# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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
from pydoop.mapreduce.simulator import HadoopSimulatorLocal, HadoopSimulatorNetwork
from avro.datafile import DataFileWriter
import avro.schema
from avro.io import DatumWriter
import StringIO
import os
import sys
CDIR = os.path.dirname(__file__)
sys.path.append(CDIR)
import avro_mapred


AVRO_INPUT_SCHEMA = """
{
  "type": "record",
  "name": "City",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "population", "type": "int"}
  ]
}
"""
AVRO_DATA_IN = [
    {'name': 'Duckburg', 'population': 1000},
    {'name': 'Mouseton', 'population': 2000}
]


class BaseTestSimulator(object):
    def __init__(self):
        self.avro_in_filename = '/tmp/pydoop-test-avro_in'
        schema = avro.schema.parse(AVRO_INPUT_SCHEMA)
        writer = DataFileWriter(open(self.avro_in_filename, 'wb'), DatumWriter(), schema)
        for city in AVRO_DATA_IN:
            writer.append(city)
        writer.close()

    def get_simulator(self):
        raise NotImplementedError

    def test_avro_input(self):
        hs = self.get_simulator()
        job_conf = {
            'pydoop.mapreduce.avro.input': 'v',
            'pydoop.mapreduce.avro.value.input.schema': AVRO_INPUT_SCHEMA
        }
        output_filename = '/tmp/pydoop-test-avro_out'
        hs.run(open(self.avro_in_filename), open(output_filename, 'w'), job_conf, 1)

        with open(output_filename) as f:
            self.assertEqual(int(f.read().strip()), sum(d['population'] for d in AVRO_DATA_IN))


class TestLocalSimulator(unittest.TestCase, BaseTestSimulator):
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        BaseTestSimulator.__init__(self)

    def get_simulator(self):
        return HadoopSimulatorLocal(factory=avro_mapred.FACTORY, context_cls=avro_mapred.CONTEXT)


# class TestNetworkSimulator(unittest.TestCase, BaseTestSimulator):
#     def __init__(self, *args, **kwargs):
#         unittest.TestCase.__init__(self, *args, **kwargs)
#         BaseTestSimulator.__init__(self)
#
#     def get_simulator(self):
#         return HadoopSimulatorNetwork(program=os.path.join(CDIR, 'avro_mapred.py'))
