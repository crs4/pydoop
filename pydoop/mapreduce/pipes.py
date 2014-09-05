# BEGIN_COPYRIGHT
#
# Copyright 2009-2014 CRS4.
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

import sys
import logging
import time
import StringIO

from pydoop.utils.serialize import deserialize_text, deserialize_long
from pydoop.utils.serialize import deserialize_old_style_filename
from pydoop.utils.serialize import serialize_to_string, serialize_text
from pydoop.utils.serialize import serialize_long

import connections
from pydoop.mapreduce.api import JobConf, RecordWriter, MapContext, ReduceContext
from pydoop.mapreduce.api import PydoopError
from pydoop.mapreduce.streams import get_key_value_stream, get_key_values_stream
from string_utils import create_digest
from pydoop.mapreduce.api import Counter
from environment_keys import *
from pydoop.utils.serialize import private_encode

from pydoop import hadoop_version_info
from pydoop.mapreduce.api import Factory as FactoryInterface

#logging.basicConfig()
logger = logging.getLogger('pipes')
logger.setLevel(logging.CRITICAL)

class Factory(FactoryInterface):
    def __init__(self, mapper_class, reducer_class=None, combiner_class=None,
                 partitioner_class=None,
                 record_writer_class=None, record_reader_class=None):
        self.mclass = mapper_class
        self.rclass = reducer_class
        self.cclass = combiner_class
        self.pclass = partitioner_class
        self.rwclass = record_writer_class
        self.rrclass = record_reader_class

    def create_mapper(self, context):
        return self.mclass(context)

    def create_reducer(self, context):
        return None if not self.rclass else self.rclass(context)

    def create_combiner(self, context):
        return None if not self.cclass else self.cclass(context)

    def create_partitioner(self, context):
        return None if not self.pclass else self.pclass(context)

    def create_record_reader(self, context):
        return None if not self.rrclass else self.rrclass(context)

    def create_record_writer(self, context):
        return None if not self.rwclass else self.rwclass(context)


class InputSplit(object):
    """
    Represents the data to be processed by an individual :class:`Mapper`\ .

    Typically, it presents a byte-oriented view on the input and it is
    the responsibility of the :class:`RecordReader` to convert this to a
    record-oriented view.

    The ``InputSplit`` is a *logical* representation of the actual
    dataset chunk, expressed through the ``filename``, ``offset`` and
    ``length`` attributes.

    :param data: the byte string returned by :meth:`MapContext.getInputSplit`
    :type data: string
    """

    def __init__(self, data):
        stream = StringIO.StringIO(data)
        if hadoop_version_info().has_variable_isplit_encoding():
            self.filename = deserialize_text(stream)
        else:
            self.filename = deserialize_old_style_filename(stream)
        self.offset = deserialize_long(stream)
        self.length = deserialize_long(stream)
        
    @classmethod
    def to_string(cls, filename, offset, length):
        stream = StringIO.StringIO()
        if hadoop_version_info().has_variable_isplit_encoding():
            serialize_text(filename, stream)
        else:
            serialize_old_style_filename(filename, stream)
        serialize_long(offset, stream)
        serialize_long(length, stream)
        return stream.getvalue()
        
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
        for key, values in self.data.iteritems():
            ctx._key, ctx._values = key, iter(values)
            self.reducer.reduce(ctx)
        ctx.writer = writer
        self.data.clear()
        self.used_bytes = 0


class TaskContext(MapContext, ReduceContext):
    def __init__(self, up_link, no_private_encoding=False):
        self.no_private_encoding = no_private_encoding
        self._private_encoding = False
        self.up_link = up_link
        self.writer = None
        self.partitioner = None
        self._job_conf = None
        self._key = None
        self._value = None
        self.n_reduces = None
        self._values = None
        self._input_split = None
        self._input_key_class = None
        self._input_value_class = None
        self._status = None
        self._status_set = False
        self._progress_float = 0.0
        self._last_progress = 0
        self._registered_counters = []

    def enable_private_encoding(self):
        self._private_encoding = not self.no_private_encoding

    def close(self):
        if self.writer:
            self.writer.close()
        self.up_link.send('done')

    def set_combiner(self, factory, input_split, n_reduces):
        #self._input_split = input_split
        self.n_reduces = n_reduces
        if self.n_reduces > 0:
            self.partitioner = factory.create_partitioner(self)
            reducer = factory.create_combiner(self)
            spill_size = self._job_conf.get_int(MAPREDUCE_TASK_IO_SORT_MB_KEY,
                                                MAPREDUCE_TASK_IO_SORT_MB)
            self.writer = CombineRunner(spill_size * 1024 * 1024,
                                        self, reducer) if reducer else None

    def emit(self, key, value):
        logger.debug("Emitting... %r,%r" % (key, value))
        self.progress()
        if self.writer:
            self.writer.emit(key, value)
        else:
            if self._private_encoding:
                key = private_encode(key)
                value = private_encode(value)
            if self.partitioner:
                part = self.partitioner.partition(key, self.n_reduces)
                self.up_link.send('partitionedOutput', part, key, value)
            else:
                logger.debug("** Sending: %r,%r" % (key, value))
                self.up_link.send('output', key, value)

    def set_job_conf(self, vals):
        self._job_conf = JobConf(vals)

    def get_job_conf(self):
        return self._job_conf

    def get_input_key(self):
        return self._key

    def get_input_value(self):
        return self._value

    def get_input_values(self):
        return self._values

    def progress(self):
        if not self.up_link:
            logger.debug("UpLink is None")
            return
        now = int(round(time.time() * 1000))
        if now - self._last_progress > 1000:
            self._last_progress = now
            if self._status_set:
                self.up_link.send("status", self._status)
                logger.debug("Sending status %r" % self._status)
                self._status_set = False
            self.up_link.send("progress", self._progress_float)
            logger.debug("Sending progress float %r" % self._progress_float)

    def set_status(self, status):
        self._status = status
        self._status_set = True
        self.progress()

    def get_counter(self, group, name):
        counter_id = len(self._registered_counters)
        self._registered_counters.append(counter_id)
        self.up_link.send("registerCounter", counter_id, group, name)
        return Counter(counter_id)

    def increment_counter(self, counter, amount):
        self.up_link.send("incrementCounter", counter.get_id(), amount)

    def get_input_split(self):
        return InputSplit(self._input_split)

    def getInputSplit(self):
        return self._input_split

    def get_input_key_class(self):
        return self._input_key_class

    def get_input_value_class(self):
        return self._input_value_class

    def next_value(self):
        try:
            logger.debug("%s" % self._values)
            self._value = self._values.next()
            return True
        except StopIteration:
            return False


def resolve_connections(port=None, istream=None, ostream=None,
                        cmd_file=None,
                        cmd_port_key=None,
                        cmd_file_key=None):
    """
    Select appropriate connection streams and protocol.
    """
    port = port if port else resolve_environment_port()
    cmd_file = cmd_file if cmd_file else resolve_environment_file()

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
        self.logger = logger.getChild('StreamRunner')        
        self.factory = factory
        self.ctx = context
        self.cmd_stream = cmd_stream
        self.password = None
        self.authenticated = False
        self.get_password()


    def get_password(self):
        secret_location_key = resolve_environment_secret_location_key()
        pfile_name = resolve_environment_secret_location(secret_location_key)
        self.logger.debug('{}:{}'.format(secret_location_key, pfile_name))
        if pfile_name is None:
            self.password = None
            return
        try:
            with open(pfile_name) as f:
                self.password = f.read()
                self.logger.debug('password:{}'.format(self.password))
        except IOError:
            self.logger.error('Could not open the password file')

    def run(self):
        self.logger.debug('start running')
        for cmd, args in self.cmd_stream:
            self.logger.debug('dispatching cmd:%s, args: %s' % (cmd, args))
            if cmd == 'authenticationReq':
                digest, challenge = args
                self.logger.debug(
                    'authenticationReq: {}, {}'.format(digest, challenge))
                if self.fails_to_authenticate(digest, challenge):
                    self.logger.critical(
                        'Server failed to authenticate. Exiting')
                    break  # bailing out
            elif cmd == 'setJobConf':
                self.ctx.set_job_conf(args)
            elif cmd == 'runMap':
                input_split, n_reduces, piped_input = args
                self.run_map(input_split, n_reduces, piped_input)
                break  # we can bail out, there is nothing more to do.
            elif cmd == 'runReduce':
                part, piped_output = args
                self.run_reduce(part, piped_output)
                break  # we can bail out, there is nothing more to do.
        self.logger.debug('done running')

    def fails_to_authenticate(self, digest, challenge):
        if self.password is None:
            self.logger.info('No password, I assume we are in playback mode')
            self.authenticated = True
            return False
        expected_digest = create_digest(self.password, challenge)
        if expected_digest != digest:
            return True
        self.authenticated = True
        response_digest = create_digest(self.password, digest)
        self.logger.debug('authenticationResp: {}'.format(response_digest))
        self.ctx.up_link.send('authenticationResp', response_digest)
        return False

    def run_map(self, input_split, n_reduces, piped_input):
        self.logger.debug('start run_map')
        factory, ctx = self.factory, self.ctx

        if n_reduces > 0:
            ctx.enable_private_encoding()

        ctx._input_split = input_split
        logger.debug("InputSPlit setted %r" % input_split)
        if piped_input:
            cmd, args = self.cmd_stream.next()
            if cmd == "setInputTypes":
                ctx._input_key_class, ctx._input_value_class = args

        logger.debug("After setInputTypes: %r, %r" % (ctx.input_key_class, ctx.input_value_class))

        reader = factory.create_record_reader(ctx)
        if reader is None and piped_input is None:
            raise PydoopError('RecordReader not defined')
        send_progress = False #reader is not None

        mapper = factory.create_mapper(ctx)
        reader = reader if reader else get_key_value_stream(self.cmd_stream)
        ctx.set_combiner(factory, input_split, n_reduces)

        for ctx._key, ctx._value in reader:
            logger.debug("key: %r, value: %r " % (ctx.key, ctx.value))
            if send_progress:
                ctx._progress_float = reader.get_progress()
                logger.debug("Progress updated to %r " % ctx._progress_float)
                ctx.progress()
            mapper.map(ctx)
        mapper.close()
        self.logger.debug('done run_map')

    def run_reduce(self, part, piped_output):
        self.logger.debug('start run_reduce')
        factory, ctx = self.factory, self.ctx
        writer = factory.create_record_writer(ctx)
        if writer is None and piped_output is None:
            raise PydoopError('RecordWriter not defined')

        ctx.writer = writer
        reducer = factory.create_reducer(ctx)
        kvs_stream = get_key_values_stream(self.cmd_stream,
                                           ctx.no_private_encoding)
        for ctx._key, ctx._values in kvs_stream:
            reducer.reduce(ctx)
        reducer.close()
        self.logger.debug('done run_reduce')


def run_task(factory, port=None, istream=None, ostream=None,
             no_private_encoding=False):
    #try:
        connections = resolve_connections(port,
                                          istream=istream, ostream=ostream)
        context = TaskContext(connections.up_link, no_private_encoding)
        stream_runner = StreamRunner(factory, context, connections.cmd_stream)
        stream_runner.run()
        context.close()
        connections.close()
        return True
    # except StandardError as e:
    #     sys.stderr.write('Hadoop Pipes Exception: %r' % e)
    #     return False


