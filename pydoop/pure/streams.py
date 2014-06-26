from abc import ABCMeta, abstractmethod


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
        self.stream = stream
        self.lifo = []

    def __iter__(self):
        return self

    def next(self):
        if self.lifo:
            return self.lifo.pop()
        return self.stream.next()

    def push_back(self, v):
        self.lifo.append(v)


class KeyValuesStream(object):

    def __init__(self, stream):
        self.stream = PushBackStream(stream)

    def __iter__(self):
        return self

    def next(self):
        for cmd, args in self.stream:
            if cmd == 'close':
                raise StopIteration
            elif cmd == 'reduceKey':
                values_stream = self.get_value_stream(self.stream)
                return args[0], values_stream
            elif cmd == 'reduceValue':
                continue
            else:
                raise ProtocolError('out of order command: {}'.format(cmd))
        raise StopIteration

    @staticmethod
    def get_value_stream(stream):
        for cmd, args in stream:
            if cmd == 'close':
                raise StopIteration
            elif cmd == 'reduceValue':
                yield args[0]
            else:
                stream.push_back((cmd, args))
                raise StopIteration
        raise StopIteration


def get_key_values_stream(stream):
    return KeyValuesStream(stream)


def get_key_value_stream(stream):
    for cmd, args in stream:
        if cmd == 'close':
            raise StopIteration
        elif cmd == 'mapItem':
            yield args
        else:
            raise ProtocolError('out of order command: {}'.format(cmd))
    raise StopIteration
