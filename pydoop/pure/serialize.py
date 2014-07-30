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

"""
Serialization routines based on Hadoop's SerialUtils.cc.


Expected usage:

.. code-block:: python

   import pydoop.serialize as ps

   class Foo(object):
       def __init__(self, x, y, z):
           assert (isinstance(x, int) and isinstance(y, float)
                   and isinstance(z, str))
           self.x, self.y, self.z = x, y, z
       @classmethod
       def read(cls, stream):
           x = ps.deserialize_int(stream)
           y = ps.deserialize_float(stream)
           z = ps.deserialize_str(stream)
           return Foo(x, y, z)

   ps.register_serializer(Foo, Foo.read)
   ps.register_deserializer('org.apache.hadoop.io.VIntWritable',
                            ps.deserialize_int)

   # in the reader code:
   class Reader(..):
       def __init__(self, ctx):
           self.ctx = ctx
       def next(self):
           ....
           key = ...
           # defaults to no changes if it does not have a deserializer for that
           # type
           key = ps.deserialize(self.ctx.input_key_type, key)
           value = ps.deserialize(self.ctx.input_value_type, value)
           return key, value

   # in the TaskContext code:
   ...
   def emit(self, key, value):
       ...
       # if there is noting registered for type(key), try __str__
       up_link.send('ouput', serialize(key), serialize(value))

We also expect to be able to support something like the following::

 pydoop script my_mr.py  --type-conversion=(VIntWritable,pydoop.serialize.VInt)

"""

from __future__ import division
import struct, xdrlib


# FIXME ref implementation
def serialize_int(t, stream):
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

def read_buffer(n, stream):
    buff = stream.read(n)
    if len(buff) != n:
        raise EOFError
    return buff

def deserialize_int(stream):
    b = struct.unpack('b', read_buffer(1, stream))[0]
    if b >= -112:
        return b
    (negative, l) = (True, -120 - b) if b < -120 else (False, -112 - b)
    q = struct.unpack('>Q', '\x00' * (8 - l) + read_buffer(l, stream))[0]
    return q^-1 if negative else q

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


def serialize_string(s, stream):
    serialize_int(len(s), stream)
    if len(s) > 0:
        stream.write(s)

def deserialize_string(stream):
    l = deserialize_int(stream)
    return read_buffer(l, stream)

def serialize_bool(v, stream):
    serialize_int(int(v), stream)

def deserialize_bool(stream):
    return bool(deserialize_int(stream))


SERIALIZE_MAP = {
    int : serialize_int,
    str : serialize_string,
    float : serialize_float,
    bool : serialize_bool,
    }

DESERIALIZE_MAP = {
    int : deserialize_int,
    str : deserialize_string,
    float : deserialize_float,
    bool : deserialize_bool,
    }

def serialize(v, stream):
    return SERIALIZE_MAP[type(v)](v, stream)

def deserialize(t, stream):
    return DESERIALIZE_MAP[t](stream)




 
