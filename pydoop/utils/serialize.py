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

"""Serialization routines based on Hadoop's SerialUtils.cc.


Object serialization/deserialization will instead be implemented as follows.

.. code-block:: python

   # documentation needs to be rewritten
   pass


The idea is to mimick Hadoop writable interface, so that we can then write:

.. code-block:: python

   # in the reader code:
   class Reader(..):
       def __init__(self, ctx):
           self.ctx = ctx
       def next(self):
           ....
           key = ...
           # defaults to no changes if it does not have a deserializer for that
           # type
           key = codec.deserialize(key, self.ctx.input_key_type)
           value = codec.deserialize(value, self.ctx.input_value_type)
           return key, value

   # in the TaskContext code:
   ...
   def emit(self, key, value):
       ...
       # if there is noting registered for type(key), try __str__
       up_link.send('ouput', codec.serialize_object(key),
                    codec.serialize_object(value))
"""
import struct
import xdrlib

from .py3compat import pickle, unicode, StringIO

# FIXME ignore [F401]
import pydoop.sercore as sc
CommandWriter = sc.CommandWriter
CommandReader = sc.CommandReader
FlowReader = sc.FlowReader
FlowWriter = sc.FlowWriter
RULES = sc.RULES


class FlowReader(sc.FlowReader):

    def __init__(self, stream, is_owned=True):
        super(FlowReader, self).__init__(stream)
        # just to avoid gc if one uses the idiom FlowReader(open(...))
        self.stream = stream
        self.is_owned = is_owned

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_val, exception_tb):
        self.close()
        if self.is_owned:
            self.stream.close()
        return False

    def read(self, rule):
        rule = rule.encode('UTF-8') if isinstance(rule, unicode) else rule
        return super(FlowReader, self).read(rule)


class FlowWriter(sc.FlowWriter):

    def __init__(self, stream, is_owned=True):
        super(FlowWriter, self).__init__(stream)
        # just to avoid gc if one uses the idiom FlowWriter(open(...))
        self.stream = stream
        self.is_owned = is_owned

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_val, exception_tb):
        self.close()
        if self.is_owned:
            self.stream.close()
        return False

    def write(self, rule, data):
        def sanitize(data):
            return tuple(x.encode('utf-8')
                         if isinstance(x, unicode)
                         else sanitize(x) if isinstance(x, tuple) else x
                         for x in data)
        rule = rule.encode('UTF-8') if isinstance(rule, unicode) else rule
        super(FlowWriter, self).write((rule, sanitize(data)))


PRIVATE_PROTOCOL = pickle.HIGHEST_PROTOCOL


def private_encode(obj):
    return pickle.dumps(obj, PRIVATE_PROTOCOL)


def private_decode(s):
    return pickle.loads(s)


# The following is a reimplementation of the Hadoop Pipes c++ utils functions.
# Do not use these functions in time-critical regions.


def read_buffer(n, stream):
    buff = stream.read(n)
    if len(buff) != n:
        raise EOFError
    return buff


def serialize_int(t, stream):
    p = xdrlib.Packer()
    p.pack_int(t)
    stream.write(p.get_buffer())


def deserialize_int(stream):
    SIZE_OF_INT = 4
    buf = read_buffer(SIZE_OF_INT, stream)
    up = xdrlib.Unpacker(buf)
    return up.unpack_int()


def deserialize_long(stream):
    SIZE_OF_LONG = 8
    buf = read_buffer(SIZE_OF_LONG, stream)
    return struct.unpack('>q', buf)[0]


def serialize_long(v, stream):
    stream.write(struct.pack('>q', v))


def serialize_vint(t, stream):
    if -112 <= t <= 127:
        stream.write(struct.pack('b', t))
    else:
        L = -112
        if t < 0:
            t ^= -1  # remove sign
            L = -120  # encode negativeness in L
        size = (t.bit_length() + 7) // 8
        stream.write(struct.pack('b', L - size))
        # we assume that integers are at most long long
        stream.write(struct.pack('>Q', t)[-size:])
    return


def deserialize_vint(stream):
    b = struct.unpack('b', read_buffer(1, stream))[0]
    if b >= -112:
        return b
    (negative, l) = (True, -120 - b) if b < -120 else (False, -112 - b)
    q = struct.unpack('>Q', b'\x00' * (8 - l) + read_buffer(l, stream))[0]
    return q ^ -1 if negative else q


def serialize_float(t, stream):
    p = xdrlib.Packer()
    p.pack_float(t)
    stream.write(p.get_buffer())


def deserialize_float(stream):
    """
    **NOTE:** floats are serialized using XDR (4 bytes).  When mapped
    back to python, the float is upgraded to a double, so it could be
    slightly different than the original value.
    """
    SIZE_OF_FLOAT = 4
    buf = read_buffer(SIZE_OF_FLOAT, stream)
    up = xdrlib.Unpacker(buf)
    return up.unpack_float()


def serialize_bool(v, stream):
    serialize_vint(int(v), stream)


def deserialize_bool(stream):
    return bool(deserialize_vint(stream))


def serialize_bytes(s, stream):
    serialize_vint(len(s), stream)
    if len(s) > 0:
        stream.write(s)


def deserialize_bytes(stream):
    """
    This is the wire format generically used by hadoop pipes.
    The data length is encoded using VInt.  Note that Text is encoded in UTF-8
    (so use deserialize_text) and complex objects can implement their own
    serialization -- within the (data length, data) stream structure.
    """
    length = deserialize_vint(stream)
    return read_buffer(length, stream)


def serialize_text(s, stream):
    if isinstance(s, unicode):
        data = s.encode('UTF-8')
    else:
        data = s
    serialize_vint(len(data), stream)
    if len(data) > 0:
        stream.write(data)


def deserialize_text(stream):
    length = deserialize_vint(stream)
    return unicode(read_buffer(length, stream), 'UTF-8')


def serialize_old_style_filename(s, stream):
    if isinstance(s, unicode):
        data = s.encode('UTF-8')
    else:
        data = s
    stream.write(struct.pack('>H', len(data)))
    if len(data) > 0:
        stream.write(data)


def deserialize_old_style_filename(stream):
    length = struct.unpack('>H', read_buffer(2, stream))[0]
    return unicode(read_buffer(length, stream), 'UTF-8')


SERIALIZE_MAP = {
    int: serialize_int,
    float: serialize_float,
    bool: serialize_bool,
    str: serialize_text,
    bytes: serialize_text,
    unicode: serialize_text
}


def serialize(v, stream, type_id=None):
    t = type(v) if type_id is None else type_id
    SERIALIZE_MAP[t](v, stream)


def serialize_to_string(v, type_id=None):
    f = StringIO()
    serialize(v, f, type_id)
    return f.getvalue()
