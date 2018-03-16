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

import pydoop.mapreduce.streams as streams

JOB_CONF = (
    'k', 'v',
    'mapreduce.job.inputformat.class', 'foo',
    'mapreduce.pipes.isjavarecordreader', 'true',
    'mapreduce.pipes.isjavarecordwriter', 'true',
)

STREAM_1_DATA = [
    (streams.MAP_ITEM, 'key1', 'val1'),
    (streams.MAP_ITEM, 'key2', 'val2'),
    (streams.MAP_ITEM, 'key3', 'val3'),
    (streams.CLOSE,),
    (streams.MAP_ITEM, 'key3', 'val3'),  # should not get here
]

STREAM_2_DATA = [
    (streams.REDUCE_KEY, 'key1'),
    (streams.REDUCE_VALUE, 'val11'),
    (streams.REDUCE_VALUE, 'val12'),
    (streams.REDUCE_VALUE, 'val13'),
    (streams.REDUCE_KEY, 'key2'),
    (streams.REDUCE_VALUE, 'val21'),
    (streams.REDUCE_VALUE, 'val22'),
    (streams.REDUCE_VALUE, 'val23'),
    (streams.CLOSE,),
    (streams.REDUCE_VALUE, 'val24'),  # should not get here
]


STREAM_3_DATA = [
    (streams.START_MESSAGE, 0),
    (streams.SET_JOB_CONF,) + JOB_CONF,
    (streams.RUN_MAP, 'input_split', 0, 1),
    (streams.SET_INPUT_TYPES, 'key_type', 'value_type'),
    (streams.MAP_ITEM, 'key1', 'the blue fox jumps on the table'),
    (streams.MAP_ITEM, 'key1', 'a yellow fox turns around'),
    (streams.MAP_ITEM, 'key2', 'a blue yellow fox sits on the table'),
    (streams.RUN_REDUCE, 0, 0),
    (streams.REDUCE_KEY, 'key1'),
    (streams.REDUCE_VALUE, 'val1'),
    (streams.REDUCE_VALUE, 'val2'),
    (streams.REDUCE_KEY, 'key2'),
    (streams.REDUCE_VALUE, 'val3'),
    (streams.CLOSE,),
]

STREAM_4_DATA = [
    (streams.OUTPUT, 'key1', 'val1'),
    (streams.PARTITIONED_OUTPUT, 22, 'key2', 'val2'),
    (streams.STATUS, 'jolly good'),
    (streams.PROGRESS, 0.99),
    (streams.DONE,),
    (streams.REGISTER_COUNTER, 22, 'cgroup', 'cname'),
    (streams.INCREMENT_COUNTER, 22, 123),
]

STREAM_5_DATA = [
    (streams.START_MESSAGE, 0),
    (streams.SET_JOB_CONF,) + JOB_CONF,
    (streams.RUN_MAP, 'input_split', 0, 1),
    (streams.SET_INPUT_TYPES, 'key_type', 'value_type'),
    (streams.MAP_ITEM, 'key1', 'the blue fox jumps on the table'),
    (streams.MAP_ITEM, 'key1', 'a yellow fox turns around'),
    (streams.MAP_ITEM, 'key2', 'a blue yellow fox sits on the table'),
    (streams.CLOSE,),
]

STREAM_6_DATA = [
    (streams.START_MESSAGE, 0),
    (streams.SET_JOB_CONF,) + JOB_CONF,
    (streams.RUN_MAP, 'input_split', 1, 1),
    (streams.SET_INPUT_TYPES, 'key_type', 'value_type'),
    (streams.MAP_ITEM, 'key1', 'the blue fox jumps on the table'),
    (streams.MAP_ITEM, 'key1', 'a yellow fox turns around'),
    (streams.MAP_ITEM, 'key2', 'a blue yellow fox sits on the table'),
    (streams.CLOSE,),
]
