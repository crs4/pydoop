# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

"""
This module provides basic, stand-alone Hadoop simulators for
debugging support.
"""

import SocketServer
import threading
import os
import tempfile
import uuid
import cStringIO
import logging

logging.basicConfig()
LOGGER = logging.getLogger('simulator')
LOGGER.setLevel(logging.CRITICAL)
# threading._VERBOSE = True

from pydoop.utils.serialize import serialize_to_string
from pydoop.sercore import fdopen as ph_fdopen

from .pipes import TaskContext, StreamRunner
from .api import RecordReader, PydoopError
from .binary_streams import (
    BinaryWriter, BinaryDownStreamFilter, BinaryUpStreamDecoder
)
from .string_utils import create_digest
from .connections import BUF_SIZE


CMD_PORT_KEY = "mapreduce.pipes.command.port"
CMD_FILE_KEY = "mapreduce.pipes.commandfile"
SECRET_LOCATION_KEY = 'hadoop.pipes.shared.secret.location'
TASK_PARTITION_V1 = 'mapred.task.partition'
TASK_PARTITION_V2 = 'mapreduce.task.partition'
OUTPUT_DIR_V1 = 'mapred.work.output.dir'
OUTPUT_DIR_V2 = 'mapreduce.task.output.dir'
DEFAULT_SLEEP_DELTA = 3


class TrivialRecordWriter(object):

    def __init__(self, simulator, stream):
        self.stream = stream
        self.logger = LOGGER.getChild('TrivialRecordWriter')
        self.simulator = simulator

    def output(self, key, value):
        self.stream.write('{}\t{}\n'.format(key, value))

    def send(self, cmd, *vals):
        if cmd == 'output':
            key, value = vals
            self.output(key, value)
        elif cmd == 'progress':
            self.simulator.set_progress(*vals)
        elif cmd == 'status':
            self.simulator.set_status(*vals)
        elif cmd == 'registerCounter':
            self.simulator.register_counter(*vals)
        elif cmd == 'incrementCounter':
            self.simulator.increment_counter(*vals)
        elif cmd == 'done':
            self.stream.close()
        else:
            raise PydoopError('Cannot manage {}'.format(cmd))

    def flush(self):
        pass

    def close(self):
        self.stream.close()


def reader_iterator(max=10):
    for i in range(1, max+1):
        yield i, "The string %s" % i


class TrivialRecordReader(RecordReader):

    def __init__(self, context):
        self.context = context
        self.max = 10
        self.current = None
        self.iter = reader_iterator(self.max)

    def __iter__(self):
        return self

    def close(self):
        pass

    def get_progress(self):
        return 0 if not self.current else float(self.current[0])/self.max

    def next(self):
        self.current = self.iter.next()
        return self.current


class SortAndShuffle(dict):

    def __init__(self, simulator, enable_local_counters=False):
        super(SortAndShuffle, self).__init__()
        self.simulator = simulator
        self.enable_local_counters = enable_local_counters

    def output(self, key, value):
        LOGGER.debug('SAS:output %r, %r', key, value)
        self.setdefault(key, []).append(value)

    def send(self, cmd, *args):
        LOGGER.debug('SAS:send %s %r', cmd, args)
        if cmd == 'output':
            key, value = args
            self.setdefault(key, []).append(value)
        elif cmd == 'partitionedOutput':
            part, key, value = args
            self.setdefault(key, []).append(value)
        elif cmd == 'registerCounter':
            if self.enable_local_counters:
                cid, group, name = args
                self.simulator.register_counter(cid, group, name)
        elif cmd == 'incrementCounter':
            if self.enable_local_counters:
                cid, increment = args
                self.simulator.increment_counter(cid, int(increment))

    def flush(self):
        pass

    def close(self):
        pass


class CommandThread(threading.Thread):
    def __init__(self, sync_event, down_bytes, ostream, logger):
        super(CommandThread, self).__init__()
        self.logger = logger.getChild('CommandThread')
        self.down_bytes = down_bytes
        self.ostream = ostream
        self.sync_event = sync_event
        self.logger.debug('initialized')

    def run(self):
        self.logger.debug('started runner.')
        chunk_size = 128 * 1024
        not_synced_yet = True
        while True:
            self.logger.debug('reading %s bytes from %s.', chunk_size,
                              self.down_bytes)
            buf = self.down_bytes.read(chunk_size)
            self.logger.debug('%s bytes actually read.', len(buf))
            if len(buf) == 0:
                break
            self.ostream.write(buf)
            self.ostream.flush()
            if not_synced_yet:
                not_synced_yet = False
                self.sync_event.set()
        self.logger.debug('Done.')


class ResultThread(threading.Thread):
    def __init__(self, simulator, up_bytes, ostream, logger):
        super(ResultThread, self).__init__()
        self.logger = logger.getChild('ResultThread')
        self.up_bytes = up_bytes
        self.ostream = ostream
        self.simulator = simulator
        self.logger.debug('initialized')

    def run(self):
        self.logger.debug('started runner.')
        up_cmd_stream = BinaryUpStreamDecoder(self.up_bytes)
        for cmd, args in up_cmd_stream:
            self.logger.debug('cmd: %r args:%r', cmd, args)
            if cmd == 'authenticationResp':
                self.logger.debug('got an authenticationResp: %r', args)
            elif cmd == 'output':
                key, value = args
                self.ostream.output(key, value)
                self.logger.debug('output: (%r, %r)', key, value)
            elif cmd == 'partitionedOutput':
                part, key, value = args
                self.ostream.output(key, value)
                self.logger.debug(
                    'partitionedOutput: (%r, %r, %r)', part, key, value
                )
            elif cmd == 'done':
                if self.ostream:
                    self.ostream.close()
                    self.logger.debug('closed ostream')
                break
            elif cmd == 'progress':
                self.simulator.set_progress(*args)
            elif cmd == 'status':
                self.simulator.set_status(*args)
            elif cmd == 'registerCounter':
                self.simulator.register_counter(*args)
            elif cmd == 'incrementCounter':
                self.simulator.increment_counter(*args)
        self.logger.debug('Done.')


class HadoopThreadHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        self.server.logger.debug('handler started')
        # We have to wait for the cmd flux to start, otherwise it appears that
        # socket data flux gets confused on what is waiting what.
        cmd_flux_has_started = threading.Event()
        fd = self.request.fileno()
        cmd_thread = CommandThread(
            cmd_flux_has_started, self.server.down_bytes,
            ph_fdopen(os.dup(fd), 'w', BUF_SIZE), self.server.logger
        )
        res_thread = ResultThread(self.server.simulator,
                                  ph_fdopen(os.dup(fd), 'r', BUF_SIZE),
                                  self.server.out_writer,
                                  self.server.logger)
        cmd_thread.start()
        cmd_flux_has_started.wait()
        res_thread.start()
        self.server.logger.debug('Waiting in cmd_thread.join().')
        cmd_thread.join()
        self.server.logger.debug('Waiting in res_thread.join().')
        res_thread.join()
        self.server.logger.debug('handler is done.')


class HadoopServer(SocketServer.TCPServer):
    r"""
    A fake Hadoop server for debugging support.
    """
    def __init__(self, simulator, port, down_bytes, out_writer,
                 host='localhost', logger=None, loglevel=logging.CRITICAL):
        """
        down_bytes is the stream of bytes produced by the binary encoding
        of a command stream.

        out_writer is an object with a .send() method that can handle 'output'
        and  'done' commands.
        """
        self.logger = logger.getChild('HadoopServer') if logger \
            else logging.getLogger('HadoopServer')
        self.logger.setLevel(loglevel)
        self.simulator = simulator
        self.down_bytes = down_bytes
        self.out_writer = out_writer
        # old style class
        SocketServer.TCPServer.__init__(
            self, (host, port), HadoopThreadHandler
        )
        self.logger.debug('initialized on (%r, %r)', host, port)

    def get_port(self):
        return self.socket.getsockname()[1]


class HadoopSimulator(object):
    r"""
    Common HadoopSimulator components.
    """

    def __init__(self, logger, loglevel=logging.CRITICAL):
        self.logger = logger
        self.logger.setLevel(loglevel)
        self.counters = {}
        self.progress = 0
        self.status = 'Undefined'
        self.phase = 'Undefined'
        self.logger.debug('initialized.')

    def set_phase(self, phase):
        self.phase = phase

    def set_progress(self, value):
        if value != self.progress:
            self.logger.info('progress: %s', value)
            self.progress = value

    def set_status(self, msg):
        if msg != self.status:
            self.logger.info('status: %s', msg)
            self.status = msg

    def register_counter(self, cid, group, name):
        self.logger.debug('registering counter[%s] (%s, %s)', cid, group, name)
        self.counters[(self.phase, cid)] = [(group, name), 0]

    def increment_counter(self, cid, increment):
        self.logger.debug(
            'incrementing counter[%s] by %s', (self.phase, cid), increment
        )
        self.counters[(self.phase, cid)][1] += increment

    def get_counters(self):
        r"""
         Extract counters information accumulated by this simulator instance.
         The expected usage is as follows::

         .. code-block:: python

          counters = hs.get_counters()
          for phase in ['mapping', 'reducing']:
              print "{} counters:".format(phase.capitalize())
             for group in counters[phase]:
                 print "  Group {}".format(group)
                 for c, v in counters[phase][group].iteritems():
                     print "   {}: {}".format(c, v)

        """
        ctable = {'mapping': {}, 'reducing': {}}
        for k, v in self.counters.iteritems():
            ctable.setdefault(
                k[0], {}).setdefault(v[0][0], {}).setdefault(v[0][1], v[1])
        return ctable

    def write_authorization(self, stream, authorization):
        if authorization is not None:
            digest, challenge = authorization
            stream.send('authenticationReq', digest, challenge)

    def write_header_down_stream(self, down_stream, authorization, job_conf):
        self.write_authorization(down_stream, authorization)
        down_stream.send('start', 0)
        down_stream.send('setJobConf',
                         tuple(sum([[k, v] for k, v in job_conf.iteritems()],
                                   [])))

    def write_map_down_stream(self, file_in, job_conf, num_reducers,
                              authorization=None, input_split=''):
        """
        Prepares a binary file with all the downward (from hadoop to the
        pipes program) command flow. If `file_in` is `not None`, it will
        simulate the behavior of hadoop `TextLineReader` FIXME and add to
        the command flow a mapItem instruction for each line of `file_in`.
        Otherwise, it assumes that the pipes program will use the
        `input_split` variable and take care of record reading by itself.
        """
        input_key_type = 'org.apache.hadoop.io.LongWritable'
        input_value_type = 'org.apache.hadoop.io.Text'
        piped_input = file_in is not None
        self.tempf = tempfile.NamedTemporaryFile('r+', prefix='pydoop-tmp')
        f = self.tempf.file
        self.logger.debug('writing map input data in %s', f.name)
        down_stream = BinaryWriter(f)
        self.write_header_down_stream(down_stream, authorization, job_conf)
        down_stream.send('runMap', input_split, num_reducers, piped_input)
        if piped_input:
            down_stream.send('setInputTypes', input_key_type, input_value_type)
            pos = file_in.tell()
            for l in file_in:
                self.logger.debug("Line: %s", l)
                k = serialize_to_string(pos)
                down_stream.send('mapItem', k, l)
                pos = file_in.tell()
            down_stream.send('close')
        self.logger.debug('\tdone writing, rewinding')
        f.seek(0)
        return f

    def write_reduce_down_stream(self, sas, job_conf, reducer,
                                 piped_output=True, authorization=None):
        """
        FIXME
        """
        self.tempf = tempfile.NamedTemporaryFile('r+', prefix='pydoop-tmp')
        f = self.tempf.file
        down_stream = BinaryWriter(f)

        self.write_header_down_stream(down_stream, authorization, job_conf)

        down_stream.send('runReduce', reducer, piped_output)
        for k in sas:
            self.logger.debug("key: %r", k)
            down_stream.send('reduceKey', k)
            for v in sas[k]:
                down_stream.send('reduceValue', v)
        down_stream.send('close')
        f.seek(0)
        return f


class HadoopSimulatorLocal(HadoopSimulator):
    r"""
    Simulates the invocation of program components in a Hadoop workflow.

    .. code-block:: python

      from my_mr_app import Factory
      hs = HadoopSimulatorLocal(Factory())
      job_conf = {...}
      hs.run(fin, fout, job_conf)
      counters = hs.get_counters()
    """

    def __init__(self, factory, logger=None, loglevel=logging.CRITICAL):
        logger = logger.getChild('HadoopSimulatorLocal') if logger \
            else logging.getLogger(self.__class__.__name__)
        super(HadoopSimulatorLocal, self).__init__(logger, loglevel)
        self.factory = factory

    def run_task(self, dstream, ustream):
        self.logger.debug('run task')
        context = TaskContext(ustream)
        self.logger.debug('got context')
        stream_runner = StreamRunner(self.factory, context, dstream)
        self.logger.debug('got runner, ready to run.')
        stream_runner.run()
        self.logger.debug('done running!')
        context.close()

    def run(self, file_in, file_out, job_conf, num_reducers=1, input_split=''):
        r"""
        Run the simulator as configured by ``job_conf``, with
        ``num_reducers`` reducers.  If ``file_in`` is not :obj:`None`,
        simulate the behavior of Hadoop's ``TextLineReader``, creating
        a record for each line in ``file_in``.  Otherwise, assume that
        the ``factory`` argument given to the constructor defines a
        :class:`~.api.RecordReader`, and that ``job_conf`` provides a
        suitable :class:`~.pipes.InputSplit`.  Similarly, if
        ``file_out`` is :obj:`None`, assume that ``factory`` defines a
        :class:`~.api.RecordWriter` with appropriate parameters in
        ``job_conf``.
        """
        self.logger.debug('run start')
        bytes_flow = self.write_map_down_stream(
            file_in, job_conf, num_reducers, input_split=input_split
        )
        dstream = BinaryDownStreamFilter(bytes_flow)
        # FIXME this is a quick hack to avoid crashes with user defined
        # RecordWriter
        f = cStringIO.StringIO() if file_out is None else file_out
        rec_writer_stream = TrivialRecordWriter(self, f)
        if num_reducers == 0:
            self.logger.info('running a map only job')
            self.set_phase('mapping')
            self.run_task(dstream, rec_writer_stream)
        else:
            self.logger.info('running a map reduce job')
            sas = SortAndShuffle(self, enable_local_counters=True)
            self.logger.info('running mapping phase')
            self.set_phase('mapping')
            self.run_task(dstream, sas)
            bytes_flow = self.write_reduce_down_stream(sas, job_conf,
                                                       num_reducers)
            rstream = BinaryDownStreamFilter(bytes_flow)
            self.logger.info('running reducing phase')
            self.set_phase('reducing')
            self.run_task(rstream, rec_writer_stream)
        self.logger.info('run done.')


class HadoopSimulatorNetwork(HadoopSimulator):
    r"""
    Simulates the invocation of program components in a Hadoop
    workflow using network connections to communicate with a
    user-provided pipes program.

    .. code-block:: python

      program_name = '../wordcount/new_api/wordcount_full.py'
      data_in = '../input/alice.txt'
      output_dir = './output'
      data_in_path = os.path.realpath(data_in)
      data_in_uri = 'file://' + data_in_path
      data_in_size = os.stat(data_in_path).st_size
      os.makedirs(output_dir)
      output_dir_uri = 'file://' + os.path.realpath(output_dir)
      conf = {
        "mapred.job.name": "wordcount",
        "mapred.work.output.dir": output_dir_uri,
        "mapred.task.partition": "0",
      }
      input_split = InputSplit.to_string(data_in_uri, 0, data_in_size)
      hsn = HadoopSimulatorNetwork(program=program_name, logger=logger,
                                   loglevel=logging.INFO)
      hsn.run(None, None, conf, input_split=input_split)

    The Pydoop application ``program`` will be launched ``sleep_delta``
    seconds after framework initialization.
    """

    def __init__(self, program=None, logger=None, loglevel=logging.CRITICAL,
                 sleep_delta=DEFAULT_SLEEP_DELTA):
        logger = logger.getChild('HadoopSimulatorNetwork') if logger \
            else logging.getLogger(self.__class__.__name__)
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
        server = HadoopServer(self, 0, down_bytes, out_writer,
                              logger=self.logger.getChild('HadoopServer'),
                              loglevel=self.logger.getEffectiveLevel())
        port = server.get_port()
        self.logger.debug('serving on port: {}'.format(port))
        self.logger.debug('secret location: {}'.format(self.tmp_file))
        os.environ[CMD_PORT_KEY] = str(port)
        os.environ[SECRET_LOCATION_KEY] = self.tmp_file
        self.logger.debug('delaying {} {} secs'.format(self.program,
                                                       self.sleep_delta))
        cmd_line = "(sleep {}; {})&".format(self.sleep_delta, self.program)
        os.system(cmd_line)
        server.handle_request()
        self.logger.debug('run_task: finished with HadoopServer')

    def run(self, file_in, file_out, job_conf, num_reducers=1, input_split=''):
        r"""
        Run the program through the simulated Hadoop infrastructure,
        piping the contents of ``file_in`` to the program similarly to
        what Hadoop's ``TextInputFormat`` does.  Setting ``file_in``
        to :obj:`None` implies that the program is expected to get its
        data from its own :class:`~.api.RecordReader`, using the
        provided ``input_split``.  Analogously, the final results will
        be written to ``file_out`` unless it is set to :obj:`None`, in
        which case the program is expected to have a
        :class:`~.api.RecordWriter`.
        """
        assert file_in or input_split
        assert file_out or num_reducers > 0  # FIXME pipes should support this
        self.logger.debug('run start')
        challenge = 'what? me worry?'
        digest = create_digest(self.password, challenge)
        auth = (digest, challenge)
        down_bytes = self.write_map_down_stream(
            file_in, job_conf, num_reducers, authorization=auth,
            input_split=input_split
        )
        record_writer = TrivialRecordWriter(
            self, file_out) if file_out else None
        if num_reducers == 0:
            self.logger.info('running a map only job')
            self.set_phase('mapping')
            self.run_task(down_bytes, record_writer)
        else:
            self.logger.info('running a map reduce job')
            sas = SortAndShuffle(self)
            self.logger.info('running mapping phase')
            self.set_phase('mapping')
            self.run_task(down_bytes, sas)
            # FIXME we only support a single reducer
            reducer_id = 1
            if not (TASK_PARTITION_V1 in job_conf or
                    TASK_PARTITION_V2 in job_conf):
                task_partition = str(reducer_id)
                job_conf[TASK_PARTITION_V1] = task_partition
                job_conf[TASK_PARTITION_V2] = task_partition
                self.logger.debug(
                    'Set %s=%s', TASK_PARTITION_V1, task_partition
                )
                self.logger.debug(
                    'Set %s=%s', TASK_PARTITION_V2, task_partition
                )
            if not (OUTPUT_DIR_V1 in job_conf or OUTPUT_DIR_V2 in job_conf):
                outdir_path = os.path.realpath(os.path.join('.', 'output'))
                outdir_uri = 'file://' + outdir_path
                job_conf[OUTPUT_DIR_V1] = outdir_uri
                job_conf[OUTPUT_DIR_V2] = outdir_uri
            down_bytes = self.write_reduce_down_stream(
                sas, job_conf, reducer_id, authorization=auth,
                piped_output=(file_out is not None)
            )
            self.logger.info('running reducer')
            self.set_phase('reducing')
            self.run_task(down_bytes, record_writer)
        self.logger.info('run done.')
        os.unlink(self.tmp_file)
