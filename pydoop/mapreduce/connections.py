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

import sys
import os
import socket
import logging

from .text_streams import TextDownStreamAdapter, TextUpStreamAdapter
from .binary_streams import BinaryDownStreamAdapter, BinaryUpStreamAdapter


logging.basicConfig(level=logging.CRITICAL)
LOGGER = logging.getLogger('connections')


BUF_SIZE = 128 * 1024


class Connections(object):

    def __init__(self, cmd_stream, up_link):
        self.cmd_stream = cmd_stream
        self.up_link = up_link

    def close(self):
        self.cmd_stream.close()
        self.up_link.flush()
        self.up_link.close()


def open_playback_connections(cmd_file, out_file, auto_serialize=True):
    in_stream = open(cmd_file, 'rb')
    out_stream = open(out_file, 'wb')
    cmd_stream = BinaryDownStreamAdapter(in_stream)
    up_link = BinaryUpStreamAdapter(out_stream, auto_serialize=auto_serialize)
    return Connections(cmd_stream, up_link)


def open_file_connections(istream=sys.stdin, ostream=sys.stdout):
    return Connections(TextDownStreamAdapter(istream),
                       TextUpStreamAdapter(ostream))


class NetworkConnections(Connections):

    def __init__(self, cmd_stream, up_link, sock, port):
        self.logger = LOGGER.getChild('NetworkConnections')
        super(NetworkConnections, self).__init__(cmd_stream, up_link)
        self.socket = sock

    def close(self):
        super(NetworkConnections, self).close()
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()


def open_network_connections(port, auto_serialize=True):
    s = socket.socket()
    s.connect(('localhost', port))
    in_stream = os.fdopen(os.dup(s.fileno()), 'r', BUF_SIZE)
    out_stream = os.fdopen(os.dup(s.fileno()), 'w', BUF_SIZE)
    cmd_stream = BinaryDownStreamAdapter(in_stream)
    up_link = BinaryUpStreamAdapter(out_stream, auto_serialize=auto_serialize)
    return NetworkConnections(cmd_stream, up_link, s, port)
