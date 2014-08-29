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
from serialize import deserialize, serialize, serialize_to_string

import logging
logging.basicConfig()
logger = logging.getLogger('binary_streams')
logger.setLevel(logging.CRITICAL)


# these constants should be exactly what has been defined in HadoopPipes.cpp
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


class BinaryWriter(object):

    CMD_CODE = {
        'start' : START_MESSAGE,
        'setJobConf' : SET_JOB_CONF,
        'setInputTypes' : SET_INPUT_TYPES,
        'runMap' : RUN_MAP,
        'mapItem' : MAP_ITEM,
        'runReduce' : RUN_REDUCE,
        'reduceKey' : REDUCE_KEY,
        'reduceValue' : REDUCE_VALUE,
        'close' : CLOSE,
        'abort' : ABORT,
        'authenticationReq' : AUTHENTICATION_REQ,
        'output' : OUTPUT,
        'partitionedOutput' : PARTITIONED_OUTPUT,
        'status' : STATUS,
        'progress' : PROGRESS,
        'done' : DONE,
        'registerCounter' : REGISTER_COUNTER,
        'incrementCounter' : INCREMENT_COUNTER,
        'authenticationResp' : AUTHENTICATION_RESP
        }

    def __init__(self, stream):
        self.stream = stream

    def send(self, *vals):
        serialize(self.CMD_CODE[vals[0]], self.stream)
        if vals[0] == 'setJobConf':
            serialize(len(vals[1:]), self.stream)
        for v in vals[1:]:
            serialize(v, self.stream)


class BinaryDownStreamFilter(DownStreamFilter):

    def get_list(stream):
        n = deserialize(int, stream)
        assert n >= 0
        return [deserialize(str, stream) for _ in range(n)]

    DFLOW_TABLE = {
        START_MESSAGE: ('start', [int], None),
        SET_JOB_CONF: ('setJobConf', None, get_list),
        SET_INPUT_TYPES: ('setInputTypes', [str, str], None),
        RUN_MAP: ('runMap', [str, int, int], None),
        MAP_ITEM: ('mapItem', [str, str], None),
        RUN_REDUCE: ('runReduce', [int, int], None),
        REDUCE_KEY: ('reduceKey', [str], None),
        REDUCE_VALUE: ('reduceValue', [str], None),
        CLOSE: ('close', [], None),
        ABORT: ('abort', [], None),
        AUTHENTICATION_REQ: ('authenticationReq', [str, str], None)
        }

    def __init__(self, stream):
        super(BinaryDownStreamFilter, self).__init__(stream)
        self.logger = logger.getChild('BinaryDownStreamFilter')

    def next(self):
        try:
            cmd_code = deserialize(int, self.stream)
        except EOFError:
            raise StopIteration
        cmd, types, processor = self.DFLOW_TABLE[cmd_code]
        if types is None: # no types, process stream directly
            args = processor(self.stream)
            return cmd, tuple(args)
        args = [deserialize(t, self.stream) for t in types]
        self.logger.debug('next -> cmd: %s; args: %s' % (cmd, args))
        return cmd, tuple(args) if args else None


class BinaryUpStreamFilter(UpStreamFilter):

    UPFLOW_TABLE =  {
        'output' : (OUTPUT, [str, str], None),
        'partitionedOutput' :(PARTITIONED_OUTPUT, [int, str, str], None),
        'status': (STATUS, [str], None),
        'progress' : (PROGRESS, [float], None),
        'done' : (DONE, [], None),
        'registerCounter' : (REGISTER_COUNTER, [int, str, str], None),
        'incrementCounter' : (INCREMENT_COUNTER, [int, int], None),
        'authenticationResp' : (AUTHENTICATION_RESP, [str], None),
        }

    def __init__(self, stream):
        super(BinaryUpStreamFilter, self).__init__(stream)
        self.logger = logger.getChild('BinaryUpStreamFilter')
        self.logger.debug('initialize on stream: %s' % stream)        

    def send(self, cmd, *args):
        self.logger.debug('cmd: %s, args: %s' % (cmd, args))
        stream = self.stream
        cmd_code, types, processor = self.UPFLOW_TABLE[cmd]
        serialize(cmd_code, stream)
        for t, v in zip(types, args):
            if t == float: # you never know...
                serialize(float(v), stream)
            elif t == int:
                assert type(v) == t
                serialize(v, stream)
            elif t == str:
                if type(v) in [str, unicode]:
                    serialize(v, stream)
                else:
                    s = serialize_to_string(v)
                    serialize(s, stream)
        stream.flush()

class BinaryUpStreamDecoder(BinaryDownStreamFilter):
    DFLOW_TABLE = dict(
        ((t[0], (k, t[1], t[2]))
         for k, t in BinaryUpStreamFilter.UPFLOW_TABLE.iteritems()))
        
