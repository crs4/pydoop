# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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

"""\
Set up communication channels with the MapReduce framework.

If "mapreduce.pipes.command.port" is in the env, this is a "real" Hadoop task:
we have to connect to the given port and use the socket for live communication
with the Java submitter.

If the above env variable is not defined, but "mapreduce.pipes.commandfile"
is, a pre-compiled binary file containing the entire command list from
upstream is available at the specified (local) filesystem path.
"""

import os
import socket

import pydoop.sercore as sercore
from .binary_protocol import Downlink, Uplink


class Connection(object):
    """\
    Create up/down links and set up references.

    The ref chain is ``downlink -> context -> uplink``, where ``downlink ->
    context`` is an owned ref and ``context -> uplink`` is a borrowed one
    (owner is responsible for closing, borrower must **not** close).

    Other refs::

      downlink -> istream (owned)
      uplink -> ostream (owned)
      connection -> downlink (owned)
      connection -> uplink (owned)

    Connection keeps no reference at all to either istream or ostream.
    """

    def __init__(self, context, istream, ostream, **kwargs):
        self.uplink = context.uplink = Uplink(ostream)
        self.downlink = Downlink(istream, context, **kwargs)

    def close(self):
        self.uplink.close()
        self.downlink.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class NetworkConnection(Connection):

    def __init__(self, context, host, port, **kwargs):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        istream = sercore.FileInStream(self.socket)
        ostream = sercore.FileOutStream(self.socket)
        super(NetworkConnection, self).__init__(
            context, istream, ostream, **kwargs
        )

    def close(self):
        super(NetworkConnection, self).close()
        self.socket.close()


class FileConnection(Connection):

    def __init__(self, context, in_fn, out_fn, **kwargs):
        istream = sercore.FileInStream(in_fn)
        ostream = sercore.FileOutStream(out_fn)
        super(FileConnection, self).__init__(
            context, istream, ostream, **kwargs
        )


def get_connection(context, **kwargs):
    port = os.getenv("mapreduce.pipes.command.port")
    if port:
        return NetworkConnection(context, "localhost", int(port), **kwargs)
    in_fn = os.getenv("mapreduce.pipes.commandfile")
    if in_fn:
        out_fn = "%s.out" % in_fn
        return FileConnection(context, in_fn, out_fn, **kwargs)
    raise RuntimeError("no pipes source found")
