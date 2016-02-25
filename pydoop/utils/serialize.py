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

"""Serialization routines based on Hadoop's SerialUtils.cc.

Client modules will typically use the ProtocolCodec interface to write binary
protocol interfaces as follows.

.. code-block:: python

   from pydoop.utils.serialize import codec
   MAP_ITEM = 4
   codec.add_rule(MAP_ITEM, 'mapItem', 'ss')
   # ... more cmd rule definitions, ...
   cmd, args = codec.deserialize_cmd(stream)


Object serialization/deserialization will instead be implemented as follows.

.. code-block:: python

   from pydoop.utils.serialize import codec, Writable

   class Foo(Writable):
       def __init__(self, x, y, z):
           assert (isinstance(x, int) and isinstance(y, float)
                   and isinstance(z, str))
           self.x, self.y, self.z = x, y, z
       def write(self, sink):
           codec.serialize(('ifs', (self.x, self.y, self.z)), sink)
       def read_fields(self, source):
           self.x, self.y, self.z = ProtocolCodec.deserialize(
               source, enc_format='ifs'
           )
       @classmethod
       def read(cls, source):
           x, y, z = codec.deserialize(source, enc_format='ifs')
           return Foo(x, y, z)

   codec.register_object('org.foo.FooObject', Foo)
   codec.register_object('org.apache.hadoop.io.VIntWritable', enc_format='i')

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

from __future__ import division
import struct
import xdrlib
from cStringIO import StringIO
import cPickle as pickle

import pydoop.sercore as codec_core


PRIVATE_PROTOCOL = pickle.HIGHEST_PROTOCOL


def private_encode(obj):
    return pickle.dumps(obj, PRIVATE_PROTOCOL)


def private_decode(s):
    return pickle.loads(s)


# The following is a reimplementation of the Hadoop Pipes c++ utils functions

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
        l = -112
        if t < 0:
            t ^= -1  # remove sign
            l = -120  # encode negativeness in l
        size = (t.bit_length() + 7) // 8
        stream.write(struct.pack('b', l - size))
        # we assume that integers are at most long long
        stream.write(struct.pack('>Q', t)[-size:])
    return


def deserialize_vint(stream):
    b = struct.unpack('b', read_buffer(1, stream))[0]
    if b >= -112:
        return b
    (negative, l) = (True, -120 - b) if b < -120 else (False, -112 - b)
    q = struct.unpack('>Q', '\x00' * (8 - l) + read_buffer(l, stream))[0]
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
    l = deserialize_vint(stream)
    return read_buffer(l, stream)


def serialize_text(s, stream):
    if isinstance(s, unicode):
        data = s.encode('UTF-8')
    else:
        data = s
    serialize_vint(len(data), stream)
    if len(data) > 0:
        stream.write(data)


def deserialize_text(stream):
    l = deserialize_vint(stream)
    return unicode(read_buffer(l, stream), 'UTF-8')


def serialize_old_style_filename(s, stream):
    if isinstance(s, unicode):
        data = s.encode('UTF-8')
    else:
        data = s
    stream.write(struct.pack('>H', len(data)))
    if len(data) > 0:
        stream.write(data)


def deserialize_old_style_filename(stream):
    l = struct.unpack('>H', read_buffer(2, stream))[0]
    return unicode(read_buffer(l, stream), 'UTF-8')


class SerializerStore(object):

    def __init__(self):
        self.serialize_map = {}
        self.deserialize_map = {}

    def register_serializer(self, class_id, ser_func):
        self.serialize_map[class_id] = ser_func

    def register_deserializer(self, class_id, deser_func):
        self.deserialize_map[class_id] = deser_func

    def serializer(self, type_id):
        return self.serialize_map[type_id]

    def deserializer(self, type_id):
        return self.deserialize_map[type_id]


DEFAULT_STORE = SerializerStore()

DEFAULT_STORE.register_serializer(int, serialize_vint)
DEFAULT_STORE.register_serializer('long', serialize_long)  # FIXME special case
DEFAULT_STORE.register_serializer(str, serialize_text)
DEFAULT_STORE.register_serializer(unicode, serialize_text)
DEFAULT_STORE.register_serializer(float, serialize_float)
DEFAULT_STORE.register_serializer(bool, serialize_bool)

DEFAULT_STORE.register_deserializer(int, deserialize_vint)
DEFAULT_STORE.register_deserializer('long', deserialize_long)
# Careful!  Use unicode if you're deserializing Text, unless you're sure it's
# ASCII!
DEFAULT_STORE.register_deserializer(str, deserialize_bytes)
DEFAULT_STORE.register_deserializer(unicode, deserialize_text)
DEFAULT_STORE.register_deserializer(float, deserialize_float)
DEFAULT_STORE.register_deserializer(bool, deserialize_bool)

DEFAULT_STORE.register_deserializer('org.apache.hadoop.io.VIntWritable',
                                    deserialize_vint)
DEFAULT_STORE.register_deserializer('org.apache.hadoop.io.VLongWritable',
                                    deserialize_vint)
DEFAULT_STORE.register_deserializer('org.apache.hadoop.io.FloatWritable',
                                    deserialize_float)
DEFAULT_STORE.register_deserializer('org.apache.hadoop.io.BooleanWritable',
                                    deserialize_bool)
DEFAULT_STORE.register_deserializer('BytesOnWire',
                                    deserialize_bytes)
# BytesWritable actually writes its length as a 4-byte integer
# (network order), but the pipes' BinaryProtocol serializes it
# "manually" rather than calling BytesWritable.write and uses a VInt
# for the size rather than a fixed-sized one.
DEFAULT_STORE.register_deserializer('org.apache.hadoop.io.BytesWritable',
                                    deserialize_bytes)
DEFAULT_STORE.register_deserializer('org.apache.hadoop.io.Text',
                                    deserialize_text)


def register_serializer(class_id, ser_func):
    DEFAULT_STORE.register_serializer(class_id, ser_func)


def register_deserializer(class_id, deser_func):
    DEFAULT_STORE.register_deserializer(class_id, deser_func)


def serialize(v, stream, type_id=None):
    if type_id is None:
        type_id = type(v)
    return DEFAULT_STORE.serializer(type_id)(v, stream)


def deserialize(type_id, stream):
    return DEFAULT_STORE.deserializer(type_id)(stream)


def serialize_to_string(v, type_id=None):
    f = StringIO()
    serialize(v, f, type_id)
    return f.getvalue()


# FIXME this is currently an almost empty shell
class ProtocolCodec(object):

    def __init__(self):
        pass

    def add_rule(self, code, name, enc_format):
        codec_core.add_rule(code, name, enc_format)

    def register_object(self, obj_name, obj_class=None, enc_format=None):
        pass

    def decode_command(self, stream):
        return codec_core.decode_command(stream)

    def encode_command(self, cmd, args, stream):
        return codec_core.encode_command(stream, cmd, args)

    @classmethod
    def serialize(cls, obj, sink=None):
        """
        .. code-block:: python
          ProtocolCodec.serialize(foo)
          ProtocolCodec.serialize(foo, sink=stream)
          ProtocolCodec.serialize(foo, sink=buffer)
          ProtocolCodec.serialize(('ifs', (2, 0.3, 'hello')), sink=buffer)
        """
        pass

    @classmethod
    def deserialize(cls, source, obj_class=None, obj_name=None,
                    enc_format=None):
        """
        .. code-block:: python
          ProtocolCodec.deserialize(source, Foo)
          ProtocolCodec.deserialize(source, obj_name='myobjs.foo')
          ProtocolCodec.deserialize(source, enc_format='ifs')
        """
        pass

codec = ProtocolCodec()
