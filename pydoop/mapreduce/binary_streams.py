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
    StreamWriter, StreamReader, DownStreamAdapter, UpStreamAdapter,
)
from pydoop.utils.serialize import CommandReader, CommandWriter, RULES
from pydoop.utils.py3compat import unicode

import logging
logging.basicConfig()
LOGGER = logging.getLogger('binary_streams')
LOGGER.setLevel(logging.CRITICAL)


class BinaryWriter(StreamWriter):

    def __init__(self, stream, auto_serialize=True):
        super(BinaryWriter, self).__init__(CommandWriter(stream))
        self.auto_serialize = auto_serialize
        self.logger = LOGGER.getChild('BinaryWriter')
        self.logger.debug('initialize on stream: %s', stream)
        # we need to be sure that stream will not be gc
        self.original_stream = stream

    def send(self, cmd, *args):
        typecodes = RULES[cmd] if cmd != self.SET_JOB_CONF else 's' * len(args)
        if self.auto_serialize:
            args = self.__to_bytes(args, typecodes)
        if cmd == self.SET_JOB_CONF:
            args = (args,)
        self.stream.write((cmd, args))

    def __to_bytes(self, args, typecodes):
        out_args = []
        for a, t in zip(args, typecodes):
            if t == "s" and not isinstance(a, (bytes, bytearray)):
                if not isinstance(a, unicode):
                    a = unicode(a)
                a = a.encode('utf-8')
            out_args.append(a)
        return tuple(out_args)


class BinaryReader(StreamReader):

    def __init__(self, stream):
        super(BinaryReader, self).__init__(CommandReader(stream))
        self.logger = LOGGER.getChild('BinaryReader')
        self.logger.debug('initialize on stream: %s', stream)
        # we need to be sure that stream will not be gc
        self.original_stream = stream

    def __iter__(self):
        self.logger.debug('requested iterator: %s', self)
        return self.stream.__iter__()

    def next(self):
        return next(self.stream)

    def __next__(self):
        return self.next()


class BinaryDownStreamAdapter(BinaryReader, DownStreamAdapter):

    def __init__(self, stream):
        super(BinaryDownStreamAdapter, self).__init__(stream)
        self.logger = LOGGER.getChild('BinaryDownStreamAdapter')
        self.logger.debug('initialize on stream: %s', stream)


class BinaryUpStreamAdapter(BinaryWriter, UpStreamAdapter):

    def __init__(self, stream, auto_serialize=True):
        super(BinaryUpStreamAdapter, self).__init__(
            stream, auto_serialize=auto_serialize
        )
        self.logger = LOGGER.getChild('BinaryUpStreamAdapter')
        self.logger.debug('initialize on stream: %s', stream)


class BinaryUpStreamDecoder(BinaryDownStreamAdapter):
    def __init__(self, stream):
        super(BinaryUpStreamDecoder, self).__init__(stream)
        self.logger = LOGGER.getChild('BinaryUpStreamDecoder')
