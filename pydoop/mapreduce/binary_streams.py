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

from .streams import DownStreamFilter, UpStreamFilter
from pydoop.utils.serialize import codec, codec_core

import logging
logging.basicConfig()
LOGGER = logging.getLogger('binary_streams')
LOGGER.setLevel(logging.CRITICAL)

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

codec.add_rule(START_MESSAGE, 'start', 'i')
codec.add_rule(SET_JOB_CONF, 'setJobConf', 'A')
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
codec.add_rule(INCREMENT_COUNTER, 'incrementCounter', 'iL')
codec.add_rule(AUTHENTICATION_RESP, 'authenticationResp', 's')


class BinaryWriter(object):

    def __init__(self, stream):
        self.stream = stream
        self.logger = LOGGER.getChild('BinaryWriter')

    def send(self, cmd, *args):
        self.logger.debug('writing %r, %r', cmd, args)
        codec.encode_command(cmd, args, self.stream)


class BinaryDownStreamFilter(DownStreamFilter):

    def __init__(self, stream):
        super(BinaryDownStreamFilter, self).__init__(stream)
        self.logger = LOGGER.getChild('BinaryDownStreamFilter')

    def __iter__(self):
        return self.fast_iterator()

    def fast_iterator(self):
        stream = self.stream
        decode_command = codec_core.decode_command
        while True:
            try:
                yield decode_command(stream)
            except EOFError:
                raise StopIteration

    def next(self):  # FIXME: this is just for timing purposes.
        try:
            return codec.decode_command(self.stream)
        except EOFError:
            raise StopIteration


class BinaryUpStreamFilter(UpStreamFilter):

    def __init__(self, stream):
        super(BinaryUpStreamFilter, self).__init__(stream)
        self.logger = LOGGER.getChild('BinaryUpStreamFilter')
        self.logger.debug('initialize on stream: %s', stream)

    def send(self, cmd, *args):
        stream = self.stream
        codec_core.encode_command(stream, cmd, args)


class BinaryUpStreamDecoder(BinaryDownStreamFilter):
    def __init__(self, stream):
        super(BinaryUpStreamDecoder, self).__init__(stream)
        self.logger = LOGGER.getChild('BinaryUpStreamDecoder')
