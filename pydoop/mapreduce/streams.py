# BEGIN_COPYRIGHT
#
# Copyright 2009-2016 CRS4.
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

from abc import ABCMeta, abstractmethod

from pydoop.utils.serialize import private_decode


class ProtocolError(Exception):
    pass


class ProtocolAbort(ProtocolError):
    pass


class StreamFilter(object):

    __metaclass__ = ABCMeta

    def __init__(self, stream):
        self.stream = stream

    def __iter__(self):
        return self

    def flush(self):
        self.stream.flush()

    def close(self):
        self.stream.close()


class DownStreamFilter(StreamFilter):

    @abstractmethod
    def next(self):
        """
        Get next command from the DownStream.  The result is in the
        form (cmd_name, args), where args could be either None or the
        command arguments tuple.
        """
        pass


class UpStreamFilter(StreamFilter):

    CMD_TABLE = {}

    def convert_message(self, cmd, args):
        pass

    @abstractmethod
    def send(self):
        pass


class PushBackStream(object):

    def __init__(self, stream):
        self.stream_iterator = stream.__iter__()
        self.lifo = []

    def __iter__(self):
        return self

    def next(self):
        if self.lifo:
            return self.lifo.pop()
        return self.stream_iterator.next()

    def push_back(self, v):
        self.lifo.append(v)


class KeyValuesStream(object):

    def __init__(self, stream, private_encoding=True):
        self.stream = PushBackStream(stream)
        self.private_encoding = private_encoding

    def __iter__(self):
        return self.fast_iterator()

    def fast_iterator(self):
        stream = self.stream
        private_encoding = self.private_encoding
        for cmd, args in stream:
            if cmd == 'close':
                raise StopIteration
            elif cmd == 'reduceKey':
                values_stream = self.get_value_stream(self.stream)
                key = private_decode(args[0]) if private_encoding else args[0]
                yield key, values_stream
            elif cmd == 'reduceValue':
                continue
            else:
                raise ProtocolError('out of order command: {}'.format(cmd))
        raise StopIteration

    def next(self):  # FIXME: only for timing comparison purposes
        for cmd, args in self.stream:
            if cmd == 'close':
                raise StopIteration
            elif cmd == 'reduceKey':
                values_stream = self.get_value_stream(self.stream)
                key = private_decode(
                    args[0]) if self.private_encoding else args[0]
                return key, values_stream
            elif cmd == 'reduceValue':
                continue
            else:
                raise ProtocolError('out of order command: {}'.format(cmd))
        raise StopIteration

    def get_value_stream(self, stream):
        private_encoding = self.private_encoding
        for cmd, args in stream:
            if cmd == 'close':
                stream.push_back((cmd, args))
                raise StopIteration
            elif cmd == 'reduceValue':
                yield private_decode(args[0]) if private_encoding else args[0]
            else:
                stream.push_back((cmd, args))
                raise StopIteration
        raise StopIteration


def get_key_values_stream(stream, private_encoding=True):
    return KeyValuesStream(stream, private_encoding)


def get_key_value_stream(stream):
    for cmd, args in stream:
        if cmd == 'close':
            raise StopIteration
        elif cmd == 'mapItem':
            yield args
        else:
            raise ProtocolError('out of order command: {}'.format(cmd))
    raise StopIteration
