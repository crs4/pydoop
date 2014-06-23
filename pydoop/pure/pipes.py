import connections

from api import Context, JobConf, PydoopError, RecordWriter
from streams import get_key_value_stream, get_key_values_stream
import sys

CMD_PORT_KEY = "mapreduce.pipes.command.port"
CMD_FILE_KEY = "mapreduce.pipes.commandfile"
MAPREDUCE_TASK_IO_SORT_MB_KEY = "mapreduce.task.io.sort.mb"
MAPREDUCE_TASK_IO_SORT_MB = 100

class CombineRunner(RecordWriter):
    def __init__(self, spill_bytes, context, reducer):
        self.spill_bytes = spill_bytes
        self.used_bytes = 0
        self.data = {}
        self.ctx = context
        self.reducer = reducer
    def emit(self, key, value):
        # FIXME I am assuming that we can neglect the dict and list overhead 
        self.used_bytes += sys.getsizeof(key)
        self.used_bytes += sys.getsizeof(value)
        self.data.setdefault(key, []).append(value)
        if self.used_bytes >= self.spill_bytes:
            self.spill_all()
    def close(self):
        self.spill_all()
    def spill_all(self):
        ctx = self.ctx        
        writer = ctx.writer
        ctx.writer = None
        for ctx.key, ctx.values in self.data.iteritems():
            self.reducer.reduce(ctx)
        ctx.writer = writer
        self.data.clear()
        self.used_bytes = 0

class TaskContext(Context, MapContext, ReduceContext):
    def __init__(self, up_link):
        self.up_link = up_link
        self.writer = None
        self.partitioner = None
        self.job_conf = None
        self.key = None
        self.value = None
        self.n_reduces = None
        self.values = None
        self.input_split = None
        self.key_class = None
        self.value_class = None
    def set_combiner(self, factory, input_split, n_reduces):
        self.input_split = input_split
        self.n_reduces = n_reduces
        if self.n_reduces > 0:
            self.partitioner = factory.create_partitioner(self)
            reducer = factory.create_combiner(self)
            spill_size = self.job_conf.get_int(MAPREDUCE_TASK_IO_SORT_MB_KEY,
                                               MAPREDUCE_TASK_IO_SORT_MB)
            self.writer = CombineRunner(spill_size * 1024 * 1024,
                                        self, reducer) if reducer else None
    def emit(self, key, value):
        self.progress()
        if self.writer:
            self.writer.emit(key, value)
        elif self.partitioner:
            part = self.partitioner.partition(key, self.n_reduces)
            self.up_link.send('partitionedOutput', key, value)
        else:
            self.up_link.send('output', key, value)            
    def set_job_conf(self, vals):
        self.job_conf = JobConf(vals)
    def get_job_conf(self):
        return self.job_conf
    def get_input_key(self):
        return self.key
    def get_input_value(self):
        return self.value
    def progress(self):
        pass
    def set_status(self, status):
        pass
    def get_counter(self, group, name):
        pass
    def increment_counter(self, counter, amount):
        pass
    def get_input_split(self):
        return self.input_split
    def get_input_key_class(self):
        return self.input_key_class
    def get_input_value_class(self):
        return self.input_value_class
    def next_value(self):
        pass

def resolve_connections(port=None, istream=None, ostream=None,
                        cmd_file=None,
                        cmd_port_key=CMD_PORT_KEY,
                        cmd_file_key=CMD_FILE_KEY):
    """
    Selects appropriate connection streams and protocol.
    """
    port = port if port else os.getenv(cmd_port_key)
    cmd_file = cmd_file if cmd_file else os.getenv(cmd_file_key)
    if port is not None:
        port = int(port)
        conn = connections.open_network_connections(port)
    elif cmd_file is not None:
        out_file = cmd_file + '.out'
        conn = connections.open_playback_connections(cmd_file, out_file)
    else:
        istream = sys.stdin if istream is None else istream
        ostream = sys.stdout if ostream is None else ostream        
        conn = connections.open_file_connections(istream=istream,
                                                 ostream=ostream)
    return conn

class StreamRunner(object):
    def __init__(self, factory, context, cmd_stream):
        self.factory = factory
        self.ctx = context
        self.cmd_stream = cmd_stream
    def run(self):
        for cmd, args in self.cmd_stream:
            if cmd == 'setJobConf':
                self.ctx.set_job_conf(args)
            elif cmd == 'runMap':
                input_split, n_reduces, piped_input = args
                self.run_map(input_split, n_reduces, piped_input)
            elif cmd == 'runReduce':
                piped_output = args
                self.run_reduce(piped_output)
    def run_map(self, input_split, n_reduces, piped_input):
        factory, ctx = self.factory, self.ctx
        reader = factory.create_record_reader(ctx)
        if reader is None and piped_input:
            raise PydoopError('RecordReader not defined')
        mapper = factory.create_mapper(ctx)
        reader = reader if reader else get_key_value_stream(self.cmd_stream)
        ctx.set_combiner(factory, input_split, n_reduces)
        for ctx.key, ctx.value in reader:
            mapper.map(ctx)
        mapper.close()
    def run_reduce(self, piped_output):
        factory, ctx = self.factory, self.ctx        
        writer = factory.create_record_writer(ctx)
        if writer is None and piped_output:
            raise PydoopError('RecordWriter not defined')
        ctx.writer = writer            
        reducer = factory.create_reducer(ctx)
        kvs_stream = get_key_values_stream(cmd_stream)
        for ctx.key, ctx.values in kvs_stream:
            reducer.reduce(ctx)
        reducer.close()


def run_task(factory, port=None, istream=None, ostream=None):
    try:
        connections = resolve_connections(port,
                                          istream=istream, ostream=ostream)
        context = TaskContext(connections.up_link)
        stream_runner = StreamRunner(factory, context, connections.cmd_stream)
        stream_runner.run()
        context.close()
        connections.shutdown()
        return True
    except Exception as e:
        sys.stderr.write('Hadoop Pipes Exception: %s' % e)
        return False
    
