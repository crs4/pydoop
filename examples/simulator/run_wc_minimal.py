# BEGIN_COPYRIGHT
#
# Copyright 2009-2014 CRS4.
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

import logging

from pydoop.mapreduce.simulator import HadoopSimulatorNetwork
from check_results import check_results

program_name = '../wordcount/new_api/wordcount-minimal.py'
data_in = '../input/alice.txt'
data_out = 'results.txt'

conf = {
    "mapred.map.tasks": "2",
    "mapred.reduce.tasks": "1",
    "mapred.job.name": "wordcount",
}
hsn = HadoopSimulatorNetwork(program=program_name, loglevel=logging.INFO)
hsn.run(open(data_in), open(data_out, 'w'), conf)

if check_results(data_in, data_out):
    print 'All is well!'
