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

import os
import socket

import pydoop.sercore as sercore
from .binary_protocol import Downlink


class Connection(object):

    def __init__(self):
        self.downlink = None

    def close(self):
        if self.downlink:
            self.downlink.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class NetworkConnection(Connection):

    def __init__(self, context, host, port, **kwargs):
        super(NetworkConnection, self).__init__()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        istream = sercore.FileInStream(self.socket)
        ostream = sercore.FileOutStream(self.socket)
        self.downlink = Downlink(istream, context, ostream, **kwargs)

    def close(self):
        super(NetworkConnection, self).close()
        self.socket.close()


class FileConnection(Connection):

    def __init__(self, context, in_fn, out_fn, **kwargs):
        super(FileConnection, self).__init__()
        istream = sercore.FileInStream(in_fn)
        ostream = sercore.FileOutStream(out_fn)
        self.downlink = Downlink(istream, context, ostream, **kwargs)


def get_connection(context, **kwargs):
    port = os.getenv("mapreduce.pipes.command.port")
    if port:
        return NetworkConnection(context, "localhost", int(port), **kwargs)
    in_fn = os.getenv("mapreduce.pipes.commandfile")
    if in_fn:
        out_fn = "%s.out" % in_fn
        return FileConnection(context, in_fn, out_fn, **kwargs)
    raise RuntimeError("no pipes source found")
