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

"""
Python equivalents for Hadoop's WritableUtils.
"""

from ..utils.py3compat import unicode
import pydoop.utils.serialize as pser
import struct


def readString(stream):
    """
    Read a string written by WritableUtils.writeString from the stream.

    The string is expected to be written as:
      * num bytes (4-byte integer, network byte order)
      * string data, encoded as UTF-8
    """
    # read the string length (4-byte int, network byte order)
    buf = stream.read(4)
    if len(buf) < 4:
        raise RuntimeError("found %d bytes (expected: 4)" % len(buf))
    n_bytes = struct.unpack("!i", buf)[0]
    if n_bytes < 0:
        return None
    buf = stream.read(n_bytes)
    if len(buf) < n_bytes:
        raise RuntimeError("found %d bytes (expected: %d)" % (
            len(buf), n_bytes
        ))
    return unicode(buf, 'UTF-8')


def readVInt(stream):
    return pser.deserialize_vint(stream)


def readVLong(stream):
    return pser.deserialize_vint(stream)


def writeString(stream, s):
    """
    Write a string to the stream as WritableUtils.writeString.
    """
    if s is None:
        stream.write(struct.pack("!i", -1))
    else:
        data = s.encode('UTF-8')
        # Write data length, as a 4-byte integer in network byte order
        stream.write(struct.pack("!i", len(data)))
        # Then write the string itself
        stream.write(data)


def writeVInt(stream, i):
    pser.serialize_vint(i, stream)


def writeVLong(stream, i):
    pser.serialize_vint(i, stream)
