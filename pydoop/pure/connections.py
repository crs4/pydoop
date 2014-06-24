from text_streams import TextDownStreamFilter, TextUpStreamFilter
from binary_streams import BinaryDownStreamFilter, BinaryUpStreamFilter

from threading import Thread, Event
import sys
import os
import socket


BUF_SIZE = 128 * 1024

class Connections(object):
    def __init__(self, cmd_stream, up_link):
        self.cmd_stream = cmd_stream
        self.up_link = up_link
    def close(self):
        self.cmd_stream.flush()
        self.cmd_stream.close()
        self.up_link.flush()
        self.up_link.close()
        

def open_playback_connections(cmd_file, out_file):
    in_stream  = open(cmd_file, 'r')
    out_stream = open(out_file, 'w')
    return Connections(BinaryDownStreamFilter(in_stream),
                       BinaryUpStreamFilter(out_stream))

def open_file_connections(istream=sys.stdin, ostream=sys.stdout):
    return Connections(TextDownStreamFilter(istream),
                       TextUpStreamFilter(ostream))

class HadoopServer(object):
    def __init__(self, port, data_file):
        """
        each line of data_file is in the form key\tvalue\n
        """
        self.port = port
        self.data_file = data_file
    def run(self):
        # listens to port for client connections
        # service client and collect output
    def run_map(self, down_stream, up_stream):
        down_stream.send('start', 0)
        down_stream.send('runMap', 'isplit', 1, False)        
        down_streams, up_streams = [down_stream], [up_stream]
        all_read, all_written = False, False
        while True:
            if all_read and all_written:
                return
            rlist, wlist, elist = select.select(up_streams, down_streams, [])
            if wlist:
                l = self.data_file.readline()
                if len(l) > 0:
                    k, v = l.strip().split('\t')
                    down_stream.send('mapItem', k, v)
                else:
                    down_stream.send('close')
                    down_streams.remove(down_stream)
                    all_written = True
            if rlist:
                cmd, args = up_stream.next()
                if cmd == 'close':
                    up_streams.remove(up_stream)
                    all_read = True
                elif cmd == 'output':
                    k, v = args
                    self.data.setdefault(k, []).append(v)
    def run_reduce(self, down_stream, up_stream):
        down_stream.send('start', 0)
        down_stream.send('runReduce', 1, 1)        
        down_streams, up_streams = [down_stream], [up_stream]
        all_read, all_written = False, False

        def iter_object():
            for k in self.data:
                yield ('reduceKey', (k,))
                for v in self.data[k]:
                    yield ('reduceValue', (v,))
        iobj = iter_object()
        while True:
            if all_read and all_written:
                return
            rlist, wlist, elist = select.select(up_streams, down_streams, [])
            if wlist:
                try:
                    (cmd, args) = iobj.next()
                    down_streams.send(cmd, args)
                except StopIteration:
                    all_written = True
                    down_stream.send('close')                    
                    down_streams.remove(down_stream)
            if rlist:
                cmd, args = up_stream.next()
                if cmd == 'close':
                    up_streams.remove(up_stream)
                    all_read = True
                elif cmd == 'output':
                    k, v = args
                    self.result[k] = v
                                    
        
class NetworkConnections(Connections):
    class LifeThread(object):
        def __init__(self, all_done, port, max_tries=3):
            self.all_done = all_done
            self.port = port
            self.max_tries = max_tries
        def __call__(self):
            while True:
                if self.all_done.wait(5):
                    break
                else:
                    for _ in range(self.max_tries):
                        try:
                            s = socket.socket()
                            s.connect(('', socket.htons(self.port)))
                            break
                        except error as e:
                            print 'error: %s' % e
                    else:
                        os._exit(1)
                    # FIXME protect with a try the next two...
                    s.shutdown(SHUT_RDWR)
                    s.close()
    def __init__(self, cmd_stream, up_link, sock, port):
        super(NetworkConnections, self).__init__(cmd_stream, up_link)
        self.all_done = Event()
        self.socket = sock
        self.life_thread = Thread(target=LifeThread(self.all_done, port))
        self.life_thread.start()
    def close(self):
        super(NetworkConnections, self).close()
        self.all_done.set()
        self.life_thread.join()
        self.socket.shutdown(SHUT_RDWR)
        self.socket.close()

def open_network_connections(port):
    s = socket.socket()
    s.connect(('', socket.htons(port))) # loopback
    in_stream  = os.fdopen(os.dup(s.fileno()), 'r', bufsize=BUF_SIZE)
    out_stream = os.fdopen(os.dup(s.fileno()), 'w', bufsize=BUF_SIZE)
    return NetworkConnections(BinaryDownStreamFilter(in_stream),
                              BinaryUpStreamFilter(out_stream), s, port)


