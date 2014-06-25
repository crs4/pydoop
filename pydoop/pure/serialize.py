from __future__ import division

import struct
import xdrlib

"""
Serialization routines based on hadoop SerialUtils.cc
"""

# FIXME ref implementation
def serialize_int(t, stream):
    if -112 <= t <= 127:
        stream.write(struct.pack('b', t))
    else:
        l = -112
        if t < 0:
            t ^= -1   # remove sign
            l = -120  # encode negativiness in l
        size = (t.bit_length() + 7)//8
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
    *NOTE* Float are serialized using XDR and there a float uses C 4bytes.
    When mapped back to python, is upgraded to a double, so it could be
    slightly different than the original value packed.
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


SERIALIZE_MAP = {int : serialize_int, str : serialize_string,
                 float : serialize_float, bool : serialize_bool}
DESERIALIZE_MAP = {int : deserialize_int, str : deserialize_string,
                   float : deserialize_float, bool : deserialize_bool}
def serialize(v, stream):
    return SERIALIZE_MAP[type(v)](v, stream)
def deserialize(t, stream):
    return DESERIALIZE_MAP[t](stream)
