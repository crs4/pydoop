from pipes import TaskContext, StreamRunner
from pydoop.pure.binary_streams import BinaryWriter, BinaryDownStreamFilter
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
        else:
            raise PydoopError('Cannot manage {}'.format(cmd))

class SortAndShuffle(dict):
    def send(self, *args):
        if args[0] == 'output':
            key, value = args[1:]
            self.setdefault(key, []).append(value)
     
class HadoopSimulatorLocal(object):
    def __init__(self, factory, logger=None, loglevel=logging.INFO):
        self.logger = logger if logger\
                             else logging.getLogger('HadoopSimulatorLocal')
        self.factory = factory
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
        return BinaryDownStreamFilter(open(fname))
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
        return BinaryDownStreamFilter(open(fname))
    def run_task(self, dstream, ustream):
        context = TaskContext(ustream)
        stream_runner = StreamRunner(self.factory, context, dstream)
        stream_runner.run()
        context.close()
    def run(self, file_in, file_out, job_conf, num_reducers):
        self.logger.debug('run start')        
        dstream = self.write_map_down_stream(file_in, job_conf, num_reducers)
        rec_writer_stream = TrivialRecordWriter(file_out)        
        if num_reducers == 0:
            self.logger.debug('running a map only job')                    
            self.run_task(dstream, rec_writer_stream)
        else:
            self.logger.debug('running a map reduce job')
            sas = SortAndShuffle()
            self.logger.debug('running mapper')            
            self.run_task(dstream, sas)
            rstream = self.write_reduce_down_stream(sas, job_conf, num_reducers)
            self.logger.debug('running reducer')                        
            self.run_task(rstream, rec_writer_stream)
        self.logger.debug('run done.')                                    
