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

from .streams import (
    StreamWriter, StreamReader,
    DownStreamAdapter, UpStreamAdapter,
    ProtocolAbort, ProtocolError
)
from .string_utils import quote_string, unquote_string


class TextWriter(StreamWriter):
    SEP = '\t'
    EOL = '\n'
    CMD_TABLE = {
        StreamWriter.START_MESSAGE: 'start',
        StreamWriter.SET_JOB_CONF: 'setJobConf',
        StreamWriter.SET_INPUT_TYPES: 'setInputTypes',
        StreamWriter.RUN_MAP: 'runMap',
        StreamWriter.MAP_ITEM: 'mapItem',
        StreamWriter.RUN_REDUCE: 'runReduce',
        StreamWriter.REDUCE_KEY: 'reduceKey',
        StreamWriter.REDUCE_VALUE: 'reduceValue',
        StreamWriter.CLOSE: 'close',
        StreamWriter.ABORT: 'abort',
        StreamWriter.AUTHENTICATION_REQ: 'authenticationReq',
        StreamWriter.OUTPUT: 'output',
        StreamWriter.PARTITIONED_OUTPUT: 'partitionedOutput',
        StreamWriter.STATUS: 'status',
        StreamWriter.PROGRESS: 'progress',
        StreamWriter.DONE: 'done',
        StreamWriter.REGISTER_COUNTER: 'registerCounter',
        StreamWriter.INCREMENT_COUNTER: 'incrementCounter',
        StreamWriter.AUTHENTICATION_RESP: 'authenticationResp',
    }

    @classmethod
    def convert_cmd(cls, cmd):
        try:
            return cls.CMD_TABLE[cmd]
        except KeyError:
            raise ProtocolError('Unrecognized command %r' % cmd)

    def __init__(self, stream):
        super(TextWriter, self).__init__(stream)

    def send(self, cmd, *args):
        self.stream.write(self.convert_cmd(cmd))
        for a in args:
            self.stream.write(self.SEP)
            self.stream.write(quote_string(str(a)))
        self.stream.write(self.EOL)


class TextReader(StreamReader):
    """
    Naive textual stream filter implementation.

    It recognizes commands and their parameters expressed as a purely textual
    down_stream flow.

    **NOTE:** this stream filter is intended for debugging purposes only.
    """
    SEP = '\t'
    CMD_TABLE = {
        'mapItem': (StreamReader.MAP_ITEM, 2, None),
        'reduceValue': (StreamReader.REDUCE_VALUE, 1, None),
        'reduceKey': (StreamReader.REDUCE_KEY, 1, None),
        'start': (StreamReader.START_MESSAGE, 1, lambda p: [int(p[0])]),
        'setJobConf': (StreamReader.SET_JOB_CONF, None, lambda p: [tuple(p)]),
        'setInputTypes': (StreamReader.SET_INPUT_TYPES, 2, None),
        'runMap': (StreamReader.RUN_MAP, 3,
                   lambda p: [p[0], int(p[1]), int(p[2])]),
        'runReduce': (StreamReader.RUN_REDUCE, 2,
                      lambda p: [int(p[0]), int(p[1])]),
        'abort': (StreamReader.ABORT, 0, None),
        'close': (StreamReader.CLOSE, 0, None),
        'output': (StreamReader.OUTPUT, 2, None),
        'partitionedOutput': (StreamReader.PARTITIONED_OUTPUT, 3,
                              lambda p: [int(p[0]), p[1], p[2]]),
        'status': (StreamReader.STATUS, 1, None),
        'progress': (StreamReader.PROGRESS, 1,
                     lambda p: [float(p[0])]),
        'done': (StreamReader.DONE, 0, None),
        'registerCounter': (StreamReader.REGISTER_COUNTER, 3,
                            lambda p: [int(p[0]), p[1], p[2]]),
        'incrementCounter': (StreamReader.INCREMENT_COUNTER, 2,
                             lambda p: [int(p[0]), int(p[1])]),
        'authenticationReq': (StreamReader.AUTHENTICATION_REQ, 2, None),
        'authenticationResp': (StreamReader.AUTHENTICATION_RESP, 1, None),
    }

    @classmethod
    def convert_message(cls, cmd, args):
        if cmd in cls.CMD_TABLE:
            cmd, nargs, converter = cls.CMD_TABLE[cmd]
            assert nargs is None or len(args) == nargs
            if cmd == cls.ABORT:
                raise ProtocolAbort('received an abort request')
            args = args if converter is None else converter(args)
            return cmd, tuple((unquote_string(a)
                               if isinstance(a, str) else a
                               for a in args)) if args else None
        else:
            raise ProtocolError('Unrecognized command %r' % cmd)

    def __init__(self, stream):
        super(TextReader, self).__init__(stream)

    def __next__(self):
        line = self.stream.readline()[:-1]
        if len(line) == 0:
            raise StopIteration
        parts = line.split(self.SEP)
        return self.convert_message(parts[0], parts[1:])

    def next(self):
        return self.__next__()


class TextUpStreamAdapter(TextWriter, UpStreamAdapter):
    pass


class TextDownStreamAdapter(TextReader, DownStreamAdapter):
    pass
