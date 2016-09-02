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


Object serialization/deserialization will instead be implemented as follows.

.. code-block:: python
   
   from pydoop.utils.serialize 

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
import struct

from .py3compat import pickle, unicode

# FIXME ignore [F401]
import pydoop.sercore as sc
CommandWriter = sc.CommandWriter
CommandReader = sc.CommandReader


PRIVATE_PROTOCOL = pickle.HIGHEST_PROTOCOL


def private_encode(obj):
    return pickle.dumps(obj, PRIVATE_PROTOCOL)


def private_decode(s):
    return pickle.loads(s)


def read_buffer(n, stream):
    buff = stream.read(n)
    if len(buff) != n:
        raise EOFError
    return buff


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
