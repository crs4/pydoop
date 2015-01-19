# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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
from threading import Thread, Event
import logging

from pydoop.sercore import fdopen as ph_fdopen
from .text_streams import TextDownStreamFilter, TextUpStreamFilter
from .binary_streams import BinaryDownStreamFilter, BinaryUpStreamFilter


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


def open_playback_connections(cmd_file, out_file):
    in_stream = open(cmd_file, 'r')
    out_stream = open(out_file, 'w')
    return Connections(BinaryDownStreamFilter(in_stream),
                       BinaryUpStreamFilter(out_stream))


def open_file_connections(istream=sys.stdin, ostream=sys.stdout):
    return Connections(TextDownStreamFilter(istream),
                       TextUpStreamFilter(ostream))


class LifeThread(object):

    def __init__(self, all_done, port, max_tries=3):
        self.all_done = all_done
        self.port = port
        self.max_tries = max_tries
        self.logger = LOGGER.getChild('LifeThread')

    def __call__(self):
        while True:
            if self.all_done.wait(5):
                break
            else:
                for _ in range(self.max_tries):
                    s = socket.socket()
                    s.connect(('localhost', self.port))
                    break
                else:
                    self.logger.critical('server appears to be dead.')
                    os._exit(1)
                # FIXME protect with a try the next two
                s.shutdown(socket.SHUT_RDWR)
                s.close()


class NetworkConnections(Connections):

    def __init__(self, cmd_stream, up_link, sock, port):
        self.logger = LOGGER.getChild('NetworkConnections')
        super(NetworkConnections, self).__init__(cmd_stream, up_link)
        self.all_done = Event()
        self.socket = sock
        self.life_thread = Thread(target=LifeThread(self.all_done, port))
        self.life_thread.start()

    def close(self):
        super(NetworkConnections, self).close()
        self.all_done.set()
        self.life_thread.join()
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()


def open_network_connections(port):
    s = socket.socket()
    s.connect(('localhost', port))
    in_stream = ph_fdopen(os.dup(s.fileno()), 'r', BUF_SIZE)
    out_stream = ph_fdopen(os.dup(s.fileno()), 'w', BUF_SIZE)
    return NetworkConnections(BinaryDownStreamFilter(in_stream),
                              BinaryUpStreamFilter(out_stream), s, port)
