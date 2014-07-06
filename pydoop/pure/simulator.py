from pydoop.pure.pipes import TaskContext, StreamRunner
from pydoop.pure.pipes import CMD_PORT_KEY, SECRET_LOCATION_KEY
from pydoop.pure.api import PydoopError
from pydoop.pure.binary_streams import BinaryWriter, BinaryDownStreamFilter
from pydoop.pure.binary_streams import BinaryUpStreamDecoder
from pydoop.pure.string_utils import create_digest
import SocketServer
import threading
import os
import tempfile
import uuid

import logging
logging.basicConfig(level=logging.CRITICAL)

DEFAULT_SLEEP_DELTA = 3

class TrivialRecordWriter(object):
    def __init__(self, stream):
        self.stream = stream
    def output(self, key, value):
        self.stream.write('{}\t{}\n'.format(key, value))
    def send(self, cmd, *vals):
        if cmd == 'output':
            key, value = vals
            self.output(key, value)
        elif cmd == 'done':
            self.stream.close()
        else:
            raise PydoopError('Cannot manage {}'.format(cmd))
    def close(self):
        self.stream.close()

class SortAndShuffle(dict):
    def output(self, key, value):
        self.setdefault(key, []).append(value)
    def send(self, *args):
        if args[0] == 'output':
            key, value = args[1:]
            self.setdefault(key, []).append(value)
    def close(self):
        pass

class CommandThread(threading.Thread):
    def __init__(self, down_bytes, ostream, logger):
        super(CommandThread, self).__init__()        
        self.down_bytes = down_bytes
        self.ostream = ostream
        self.logger = logger
        self.logger.debug('initialized')
    def run(self):
        chunk_size = 128 * 1024
        self.logger.debug('started')        
        while True:
            buf = self.down_bytes.read(chunk_size)
            if len(buf) == 0:
                break
            self.ostream.write(buf)
            self.ostream.flush()
        self.logger.debug('done')

class ResultThread(threading.Thread):
    def __init__(self, up_bytes, ostream, logger):
        super(ResultThread, self).__init__()        
        self.up_bytes = up_bytes
        self.ostream = ostream
        self.logger = logger
        self.logger.debug('initialized')
    def run(self):
        up_cmd_stream = BinaryUpStreamDecoder(self.up_bytes)
        for cmd, args in up_cmd_stream:
            self.logger.debug('cmd:{} args:{}'.format(cmd, args))            
            if cmd == 'authenticationResp':
                self.logger.debug('got an authenticationResp: {}'.format(args))
            elif cmd == 'output':
                key, value = args
                self.ostream.output(key, value)
            elif cmd == 'done':
                self.ostream.close()
                self.logger.debug('closed ostream')
                break
            elif cmd == 'progress':
                (progress,) = args
                self.logger.info('progress:{}'.format(progress))
        self.logger.debug('done with ResultThread')

            
class HadoopThreadHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        self.server.logger.debug('handler started')
        cmd_thread = CommandThread(self.server.down_bytes, self.wfile,
                                   self.server.logger.getChild('CommandThread'))
        res_thread = ResultThread(self.rfile, self.server.out_writer,
                                  self.server.logger.getChild('ResultThread'))
        cmd_thread.start()
        res_thread.start()
        cmd_thread.join()
        self.server.logger.debug('cmd_thread returned.')
        res_thread.join()
        self.server.logger.debug('res_thread returned.')        
    
class HadoopServer(SocketServer.TCPServer):
    """
    A fake Hadoop server for debugging support.

    .. code-block:: python

      port = 9999
      data_file = 'alice.txt'
      server = HadoopServer(port, data_file)
      server.serve_forever()
    
    """
    def __init__(self, port, down_bytes, out_writer,
                 host='localhost', logger=None, loglevel=logging.CRITICAL):
        """
        down_bytes is the stream of bytes produced by the binary encoding
        of a command stream.

        out_writer is an object with a .send() method that can handle 'output'
        and  'done' commands.
        """
        self.logger = logger if logger\
                             else logging.getLogger('HadoopServer')
        self.logger.setLevel(loglevel)
        self.down_bytes = down_bytes
        self.out_writer = out_writer
        # old style class
        SocketServer.TCPServer.__init__(self, (host, port), HadoopThreadHandler)
        self.logger.debug('initialized on ({}, {})'.format(host, port))

    def get_port(self):
        return self.socket.getsockname()[1]
        

#-------------------------------------------------------------------------
class HadoopSimulator(object):
    def __init__(self, logger=None, loglevel=logging.CRITICAL):
        self.logger = logger if logger\
                             else logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(loglevel)
        self.logger.debug('initialized.')

    def write_authorization(self, stream, authorization):
        if authorization is not None:
            digest, challenge = authorization
            stream.send('authenticationReq', digest, challenge)
        
    def write_map_down_stream(self, file_in, job_conf, num_reducers,
                              piped_input=False, authorization=None):
        fname = 'down_stream_map.bin'
        with open(fname, 'w') as f:
            down_stream = BinaryWriter(f)
            self.write_authorization(down_stream, authorization)
            down_stream.send('start', 0)
            down_stream.send('setJobConf',
                             *sum([[k, v] for k, v in job_conf.iteritems()],
                                  []))
            down_stream.send('runMap', 'fake_isplit', num_reducers,
                             piped_input)
            for l in file_in:
                k, v = l.strip().split('\t')
                down_stream.send('mapItem', k, v)
            down_stream.send('close')
        return open(fname)
    
    def write_reduce_down_stream(self, sas, job_conf, reducer,
                                 piped_output=False, authorization=None):
        fname = 'down_stream_reduce.bin'
        with open(fname, 'w') as f:
            down_stream = BinaryWriter(f)
            self.write_authorization(down_stream, authorization)
            down_stream.send('start', 0)
            down_stream.send('setJobConf',
                             *sum([[k, v] for k, v in job_conf.iteritems()],
                                  []))
            down_stream.send('runReduce', reducer, piped_output)
            for k in sas:
                down_stream.send('reduceKey', k)
                for v in sas[k]:
                    down_stream.send('reduceValue', v)
            down_stream.send('close')                    
        return open(fname)
    
class HadoopSimulatorLocal(HadoopSimulator):
    def __init__(self, factory, logger=None, loglevel=logging.CRITICAL):
        super(HadoopSimulatorLocal, self).__init__(logger, loglevel)
        self.factory = factory
    def run_task(self, dstream, ustream):
        context = TaskContext(ustream)
        stream_runner = StreamRunner(self.factory, context, dstream)
        stream_runner.run()
        context.close()
    def run(self, file_in, file_out, job_conf, num_reducers):
        self.logger.debug('run start')        
        bytes_flow = self.write_map_down_stream(file_in, job_conf, num_reducers)
        dstream = BinaryDownStreamFilter(bytes_flow)
        rec_writer_stream = TrivialRecordWriter(file_out)        
        if num_reducers == 0:
            self.logger.info('running a map only job')                    
            self.run_task(dstream, rec_writer_stream)
        else:
            self.logger.info('running a map reduce job')
            sas = SortAndShuffle()
            self.logger.info('running mapper')            
            self.run_task(dstream, sas)
            bytes_flow = self.write_reduce_down_stream(sas, job_conf,
                                                       num_reducers)
            rstream = BinaryDownStreamFilter(bytes_flow)            
            self.logger.info('running reducer')                        
            self.run_task(rstream, rec_writer_stream)
        self.logger.info('run done.')

class HadoopSimulatorNetwork(HadoopSimulator):
    """
    This is a debugging support simulator class that uses network connections
    to communicate to a user-provided pipes program.

    It implements a reasonably close aproximation of the 'real'
    Hadoop-pipes setup.
    """
    def __init__(self, program=None, logger=None, loglevel=logging.CRITICAL,
                 sleep_delta=DEFAULT_SLEEP_DELTA):
        super(HadoopSimulatorNetwork, self).__init__(logger, loglevel)
        self.program = program
        self.sleep_delta = sleep_delta
        tfile = tempfile.NamedTemporaryFile(delete=False)        
        self.tmp_file = tfile.name
        self.password = uuid.uuid4().hex
        tfile.write(self.password)
        tfile.close()
        
    def run_task(self, down_bytes, out_writer):
        self.logger.debug('run_task: started HadoopServer')
        server = HadoopServer(0, down_bytes, out_writer,
                              logger=self.logger.getChild('HadoopServer'),
                              loglevel=self.logger.getEffectiveLevel())
        port = server.get_port()
        self.logger.debug('serving on port: {}'.format(port))
        self.logger.debug('secret location: {}'.format(self.tmp_file))
        os.environ[CMD_PORT_KEY] = str(port)
        os.environ[SECRET_LOCATION_KEY] = self.tmp_file
        self.logger.info('delaying {} {} secs'.format(self.program,
                                                      self.sleep_delta))
        cmd_line = "(sleep {}; {})&".format(self.sleep_delta, self.program)
        os.system(cmd_line)
        server.handle_request()
        self.logger.debug('run_task: finished with HadoopServer')
                 
    def run(self, file_in, file_out, job_conf, num_reducers=1):
        self.logger.debug('run start')
        challenge = 'what? me worry?'
        digest = create_digest(self.password, challenge)
        auth = (digest, challenge)
        down_bytes = self.write_map_down_stream(file_in, job_conf, num_reducers,
                                                authorization=auth)
        record_writer = TrivialRecordWriter(file_out)
        if num_reducers == 0:
            self.logger.debug('running a map only job')                    
            self.run_task(down_bytes, record_writer)
        else:
            self.logger.debug('running a map reduce job')
            sas = SortAndShuffle()
            self.logger.debug('running mapper')
            self.run_task(down_bytes, sas)
            down_bytes = self.write_reduce_down_stream(sas, job_conf,
                                                       num_reducers,
                                                       authorization=auth)
            self.logger.debug('running reducer')            
            self.run_task(down_bytes, record_writer)
        self.logger.debug('run done.')
        os.unlink(self.tmp_file)
    
        
