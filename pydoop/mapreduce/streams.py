# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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

from abc import abstractmethod
from itertools import groupby
from operator import itemgetter

from pydoop.utils.py3compat import ABC
from pydoop.utils.serialize import private_decode


# these constants should be exactly what has been defined in
# PipesMapper.java and BinaryProtocol.java
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


class ProtocolError(Exception):
    pass


class ProtocolAbort(ProtocolError):
    pass


class StreamAdapter(ABC):

    def __init__(self, stream):
        self.stream = stream

    def __iter__(self):
        return self

    def close(self):
        self.stream.close()


class StreamWriter(StreamAdapter):
    START_MESSAGE = START_MESSAGE
    SET_JOB_CONF = SET_JOB_CONF
    SET_INPUT_TYPES = SET_INPUT_TYPES
    RUN_MAP = RUN_MAP
    MAP_ITEM = MAP_ITEM
    RUN_REDUCE = RUN_REDUCE
    REDUCE_KEY = REDUCE_KEY
    REDUCE_VALUE = REDUCE_VALUE
    CLOSE = CLOSE
    ABORT = ABORT
    AUTHENTICATION_REQ = AUTHENTICATION_REQ
    OUTPUT = OUTPUT
    PARTITIONED_OUTPUT = PARTITIONED_OUTPUT
    STATUS = STATUS
    PROGRESS = PROGRESS
    DONE = DONE
    REGISTER_COUNTER = REGISTER_COUNTER
    INCREMENT_COUNTER = INCREMENT_COUNTER
    AUTHENTICATION_RESP = AUTHENTICATION_RESP

    def flush(self):
        self.stream.flush()

    @abstractmethod
    def send(self):
        pass


class StreamReader(StreamAdapter):
    "A class for debugging purposes"
    START_MESSAGE = START_MESSAGE
    SET_JOB_CONF = SET_JOB_CONF
    SET_INPUT_TYPES = SET_INPUT_TYPES
    RUN_MAP = RUN_MAP
    MAP_ITEM = MAP_ITEM
    RUN_REDUCE = RUN_REDUCE
    REDUCE_KEY = REDUCE_KEY
    REDUCE_VALUE = REDUCE_VALUE
    CLOSE = CLOSE
    ABORT = ABORT
    AUTHENTICATION_REQ = AUTHENTICATION_REQ
    OUTPUT = OUTPUT
    PARTITIONED_OUTPUT = PARTITIONED_OUTPUT
    STATUS = STATUS
    PROGRESS = PROGRESS
    DONE = DONE
    REGISTER_COUNTER = REGISTER_COUNTER
    INCREMENT_COUNTER = INCREMENT_COUNTER
    AUTHENTICATION_RESP = AUTHENTICATION_RESP

    @abstractmethod
    def next(self):
        """
        Get next command from the DownStream.  The result is in the
        form (cmd_code, args), where args could be either None or the
        command arguments tuple.
        """
        pass


class DownStreamAdapter(StreamAdapter):
    START_MESSAGE = START_MESSAGE
    SET_JOB_CONF = SET_JOB_CONF
    SET_INPUT_TYPES = SET_INPUT_TYPES
    RUN_MAP = RUN_MAP
    MAP_ITEM = MAP_ITEM
    RUN_REDUCE = RUN_REDUCE
    REDUCE_KEY = REDUCE_KEY
    REDUCE_VALUE = REDUCE_VALUE
    CLOSE = CLOSE
    ABORT = ABORT
    AUTHENTICATION_REQ = AUTHENTICATION_REQ

    @abstractmethod
    def next(self):
        """
        Get next command from the DownStream.  The result is in the
        form (cmd_code, args), where args could be either None or the
        command arguments tuple.
        """
        pass


class UpStreamAdapter(StreamWriter):
    OUTPUT = OUTPUT
    PARTITIONED_OUTPUT = PARTITIONED_OUTPUT
    STATUS = STATUS
    PROGRESS = PROGRESS
    DONE = DONE
    REGISTER_COUNTER = REGISTER_COUNTER
    INCREMENT_COUNTER = INCREMENT_COUNTER
    AUTHENTICATION_RESP = AUTHENTICATION_RESP

    CMD_TABLE = {}

    def convert_message(self, cmd, args):
        pass


def raw_key_values_stream(stream):
    key = None
    for cmd, group in groupby(stream, itemgetter(0)):
        if cmd == CLOSE:
            raise StopIteration
        elif cmd == REDUCE_KEY:
            key = next(group)[1][0]
        else:
            yield key, (_[1][0] for _ in group)


def decoded_key_values_stream(stream):
    for k, vstream in raw_key_values_stream(stream):
        yield private_decode(k), (private_decode(_) for _ in vstream)


def get_key_values_stream(stream, private_encoding=True):
    if private_encoding:
        return decoded_key_values_stream(stream)
    else:
        return raw_key_values_stream(stream)


def get_key_value_stream(stream):
    for cmd, args in stream:
        if cmd == CLOSE:
            raise StopIteration
        elif cmd == MAP_ITEM:
            yield args
        else:
            raise ProtocolError('out of order command: {}'.format(cmd))
    raise StopIteration
