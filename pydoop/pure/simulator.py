from pipes import TaskContext, StreamRunner, CMD_PORT_KEY
from api import PydoopError
from pydoop.pure.binary_streams import BinaryWriter, BinaryDownStreamFilter
from pydoop.pure.binary_streams import BinaryUpStreamDecoder
import SocketServer
import threading

import logging
logging.basicConfig(level=logging.INFO)
        

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

class SortAndShuffle(dict):
    def output(self, key, value):
        self.setdefault(key, []).append(value)
    def send(self, *args):
        if args[0] == 'output':
            key, value = args[1:]
            self.setdefault(key, []).append(value)

class CommandThread(threading.Thread):
    def __init__(self, cmd_stream, ostream, logger):
        super(CommandThread, self).__init__()        
        self.cmd_stream = cmd_stream
        self.ostream = ostream
        self.logger = logger
        self.logger.debug('initialized')
    def run(self):
        chunk_size = 128 * 1024
        self.logger.debug('started')        
        while True:
            buf = self.cmd_stream.read(chunk_size)
            if len(buf) == 0:
                break
            self.ostream.write(buf)
            self.stream.flush()            
        self.logger.debug('done')                
            
class HadoopThreadHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        self.server.logger.debug('handler started')
        cmd_thread = CommandThread(self.server.cmd_stream, self.wfile,
                                   self.server.logger.getChild('CommandStream'))
        cmd_thread.start()
        up_cmd_stream = BinaryUpStreamDecoder(self.rfile)
        for cmd, args in up_cmd_stream:
            if cmd == 'output':
                key, value = args
                self.server.out_writer.output(key, value)
            elif cmd == 'done':
                self.server.out_writer.close()
                break
            elif cmd == 'progress':
                (progress,) = args
                self.server.logger.info('progress:{}'.format(progress))
        cmd_thread.join()
        server.shutdown()

    def run_map(self, down_stream, up_stream):
        pass
    def run_reduce(self, down_stream, up_stream):
        pass
                                    
        
    
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
                 host='localhost', logger=None, loglevel=logging.INFO):
        """
        down_bytes is the stream of bytes produced by the binary encoding
        of a command stream.

        out_writer is an object with a .send() method that can handle 'output'
        and  'done' commands.
        """
        super(HadoopServer, self).__init__((host, port), HadoopThreadHandler)
        self.logger = logger if logger\
                             else logging.getLogger('HadoopServer')
        self.logger.setLevel(loglevel)
        self.logger.debug('initialized.')
        self.down_bytes = down_bytes
        self.out_writer = out_writer
        self.num_reducers = num_reducers

#-------------------------------------------------------------------------
class HadoopSimulator(object):
    def __init__(self, logger=None, loglevel=logging.INFO):
        self.logger = logger if logger\
                             else logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(loglevel)
        self.logger.debug('initialized.')
        
    def write_map_down_stream(self, file_in, job_conf, num_reducers,
                              piped_input=False):
        fname = 'down_stream_map.bin'
        with open(fname, 'w') as f:
            down_stream = BinaryWriter(f)
            down_stream.send('start', 0)
            down_stream.send('setJobConf',
                             *sum([[k, v] for k, v in job_conf.iteritems()],
                                  []))
            down_stream.send('runMap', 'fake_isplit', num_reducers,
                             piped_input)
            for l in file_in:
                k, v = l.strip().split('\t')
                down_stream.send('mapItem', k, v)
        return open(fname)
    
    def write_reduce_down_stream(self, sas, job_conf, reducer,
                                 piped_output=False):
        fname = 'down_stream_reduce.bin'
        with open(fname, 'w') as f:
            down_stream = BinaryWriter(f)
            down_stream.send('start', 0)
            down_stream.send('setJobConf',
                             *sum([[k, v] for k, v in job_conf.iteritems()],
                                  []))
            down_stream.send('runReduce', reducer, piped_output)
            for k in sas:
                down_stream.send('reduceKey', k)
                for v in sas[k]:
                    down_stream.send('reduceValue', v)
        return open(fname)
    
class HadoopSimulatorLocal(HadoopSimulator):
    def __init__(self, factory, logger=None, loglevel=logging.INFO):
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
            self.logger.debug('running a map only job')                    
            self.run_task(dstream, rec_writer_stream)
        else:
            self.logger.debug('running a map reduce job')
            sas = SortAndShuffle()
            self.logger.debug('running mapper')            
            self.run_task(dstream, sas)
            bytes_flow = self.write_reduce_down_stream(sas, job_conf,
                                                       num_reducers)
            rstream = BinaryDownStreamFilter(bytes_flow)            
            self.logger.debug('running reducer')                        
            self.run_task(rstream, rec_writer_stream)
        self.logger.debug('run done.')

class HadoopSimulatorNetwork(HadoopSimulator):
    """
    This is a debugging support simulator class that uses network connextions
    to communicate to a user-provided pipes program.

    It implements a reasonably close aproximation of the 'real'
    Hadoop-pipes setup.

    

    """
    def __init__(self, program=None, port=9999,
                 logger=None, loglevel=logging.INFO):
        super(HadoopSimulatorNetwork, self).__init__(logger, loglevel)
        self.port = port
        self.program = program
        
    def run_task(self, down_bytes, out_writer):
        self.logger.debug('run_task: started HadoopServer')
        server = HadoopServer(self.port, down_bytes, out_writer,
                              logger=self.logger.getChild('HadoopServer'),
                              loglevel=self.logger.getEffectiveLevel())
        os.setenv(CMD_PORT_KEY, self.port)
        cmd_line = "(sleep 5; %s)&" % self.program
        sys.system(cmd_line)
        server.serve_forever() # 
        self.logger.debug('run_task: finished with HadoopServer')
                 
    def run(self, file_in, file_out, job_conf, num_reducers):
        self.logger.debug('run start')        
        down_bytes = self.write_map_down_stream(file_in, job_conf, num_reducers)
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
                                                       num_reducers)
            self.run_task(down_bytes, record_writer)
        self.logger.debug('run done.')                         
        
    
        
