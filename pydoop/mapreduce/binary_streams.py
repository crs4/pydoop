# BEGIN_COPYRIGHT
#
# Copyright 2009-2014 CRS4.
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

import sys
from streams import DownStreamFilter, UpStreamFilter
from serialize import ProtocolCodec

import logging
logging.basicConfig()
logger = logging.getLogger('binary_streams')
logger.setLevel(logging.CRITICAL)

# these constants should be exactly what has been defined in PipesMapper.java
# BinaryProtocol.java
START_MESSAGE = 0
SET_JOB_CONF = 1
SET_INPUT_TYPES = 2
RUN_MAP = 3
MAP_ITEM = 4
RUN_REDUCE = 5
REDUCE_KEY = 6
REDUCE_VALUE = 7
CLOSE = 8
ABORT = 9
AUTHENTICATION_REQ = 10
OUTPUT = 50
PARTITIONED_OUTPUT = 51
STATUS = 52
PROGRESS = 53
DONE = 54
REGISTER_COUNTER = 55
INCREMENT_COUNTER = 56
AUTHENTICATION_RESP = 57

codec = ProtocolCodec()
codec.add_rule(START_MESSAGE, 'start', 'i')
codec.add_rule(SET_JOB_CONF, 'setJobConf', 'L')
codec.add_rule(SET_INPUT_TYPES, 'setInputTypes', 'ss')
codec.add_rule(RUN_MAP, 'runMap', 'sii')
codec.add_rule(MAP_ITEM, 'mapItem', 'ss')
codec.add_rule(RUN_REDUCE, 'runReduce', 'ii')
codec.add_rule(REDUCE_KEY, 'reduceKey', 's')
codec.add_rule(REDUCE_VALUE, 'reduceValue', 's')
codec.add_rule(CLOSE, 'close', '')
codec.add_rule(ABORT, 'abort', '')
codec.add_rule(AUTHENTICATION_REQ, 'authenticationReq', 'ss')
codec.add_rule(OUTPUT, 'output', 'ss')
codec.add_rule(PARTITIONED_OUTPUT, 'partitionedOutput', 'iss')
codec.add_rule(STATUS, 'status', 's')
codec.add_rule(PROGRESS, 'progress', 'f')
codec.add_rule(DONE, 'done', '')
codec.add_rule(REGISTER_COUNTER, 'registerCounter', 'iss')
codec.add_rule(INCREMENT_COUNTER, 'incrementCounter', 'ii')
codec.add_rule(AUTHENTICATION_RESP, 'authenticationResp', 's')



class BinaryWriter(object):
    def __init__(self, stream):
        self.stream = stream
    def send(self, cmd, *args):
        codec.serialize_cmd(cmd, args)

class BinaryDownStreamFilter(DownStreamFilter):
    def __init__(self, stream):
        super(BinaryDownStreamFilter, self).__init__(stream)
        self.logger = logger.getChild('BinaryDownStreamFilter')
    def next(self):
        try:
            return codec.deserialize_cmd(self.stream)
        except EOFError:
            raise StopIteration

class BinaryUpStreamFilter(UpStreamFilter):
    def __init__(self, stream):
        super(BinaryUpStreamFilter, self).__init__(stream)
        self.logger = logger.getChild('BinaryUpStreamFilter')
        self.logger.debug('initialize on stream: %s', stream)        

    def send(self, cmd, *args):
        self.logger.debug('cmd: %s, args: %s', cmd, args)
        stream = self.stream
        codec.serialize_cmd(self.stream, cmd, args)        
        stream.flush()

class BinaryUpStreamDecoder(BinaryDownStreamFilter):
    def __init__(self, stream):
        super(BinaryUpStreamDecoder, self).__init__(stream)
        self.logger = logger.getChild('BinaryUpStreamDecoder')
