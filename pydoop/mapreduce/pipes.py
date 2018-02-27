# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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
import os
import logging
import time
import numbers
import struct
import types

from copy import deepcopy

from pydoop import hadoop_version_info
from pydoop.utils.serialize import (
    deserialize_text,
    deserialize_long,
    serialize_old_style_filename,
    deserialize_old_style_filename,
    serialize_text,
    serialize_long,
    private_encode,
)

from . import connections, api
from .streams import get_key_value_stream, get_key_values_stream
from .binary_streams import BinaryUpStreamAdapter
from .string_utils import create_digest

from pydoop.utils.py3compat import unicode, StringIO, iteritems


logging.basicConfig()
LOGGER = logging.getLogger('pipes')
LOGGER.setLevel(logging.CRITICAL)

PSTATS_DIR = "PYDOOP_PSTATS_DIR"
PSTATS_FMT = "PYDOOP_PSTATS_FMT"
DEFAULT_PSTATS_FMT = "%s_%05d_%s"  # task_type, task_id, random suffix
if os.getenv(PSTATS_DIR):
    import tempfile
    import cProfile
    import pydoop.hdfs as hdfs
DEFAULT_IO_SORT_MB = 100

_PORT_KEYS = frozenset([
    "hadoop.pipes.command.port",  # Hadoop 1
    "mapreduce.pipes.command.port",  # Hadoop 2
])
_FILE_KEYS = frozenset([
    "hadoop.pipes.command.file",  # Hadoop 1
    "mapreduce.pipes.commandfile"  # Hadoop 2.  No dot.
])
_SECRET_LOCATION_KEYS = frozenset([
    "hadoop.pipes.shared.secret.location"  # All versions
])
_INPUT_FORMAT_KEYS = frozenset([
    "mapred.input.format.class",
    "mapreduce.inputformat.class",
    "mapreduce.job.inputformat.class",
])

# FIXME: duplicate with app.submit, move to a common module
IS_JAVA_RW = "hadoop.pipes.java.recordwriter"


class LongWritableDeserializer(object):

    def __init__(self):
        self.struct = struct.Struct(">q")

    def deserialize(self, record):
        return self.struct.unpack(record)[0]


class TextDeserializer(object):

    def __init__(self):
        self.decoder = "utf-8"

    def deserialize(self, record):
        return record.decode(self.decoder)


def _get_from_env(candidate_keys):
    for k in candidate_keys:
        v = os.getenv(k)
        if v is not None:
            return v
    return None


def get_command_port():
    return _get_from_env(_PORT_KEYS)


def get_command_file():
    return _get_from_env(_FILE_KEYS)


def get_secret_location():
    return _get_from_env(_SECRET_LOCATION_KEYS)


class Factory(api.Factory):
    """
    Creates MapReduce application components.

    The classes to use for each component must be specified as arguments
    to the constructor.
    """
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
    r"""
    Represents the data to be processed by an individual :class:`Mapper`\ .

    Typically, it presents a byte-oriented view on the input and it is
    the responsibility of the :class:`RecordReader` to convert this to a
    record-oriented view.

    The ``InputSplit`` is a *logical* representation of the actual
    dataset chunk, expressed through the ``filename``, ``offset`` and
    ``length`` attributes.

    InputSplit objects are instantiated by the framework and accessed
    via :attr:`MapContext.input_split`\ .
    """
    def __init__(self, data):
        stream = StringIO(data)
        if hadoop_version_info().has_variable_isplit_encoding():
            self.filename = deserialize_text(stream)
        else:
            self.filename = deserialize_old_style_filename(stream)
        self.offset = deserialize_long(stream)
        self.length = deserialize_long(stream)

    @classmethod
    def to_string(cls, filename, offset, length):
        stream = StringIO()
        if hadoop_version_info().has_variable_isplit_encoding():
            serialize_text(filename, stream)
        else:
            serialize_old_style_filename(filename, stream)
        serialize_long(offset, stream)
        serialize_long(length, stream)
        return stream.getvalue()


class CombineRunner(api.RecordWriter):

    def __init__(self, spill_bytes, context, reducer, fast_combiner=False):
        self.spill_bytes = spill_bytes
        self.used_bytes = 0
        self.data = {}
        self.ctx = context
        self.reducer = reducer
        self.fast_combiner = fast_combiner
        self.spill_counter = self.ctx.get_counter(
            'Pydoop CombineRunner', 'spills')
        self.spilled_bytes_counter = self.ctx.get_counter(
            'Pydoop CombineRunner', 'spilled bytes')
        self.in_rec_counter = self.ctx.get_counter(
            'Pydoop CombineRunner', 'input records')

    def __defensive_copy(self, v):
        if isinstance(v, (str, unicode, numbers.Number)):
            return v
        else:
            return deepcopy(v)

    def emit(self, key, value):
        self.used_bytes += sys.getsizeof(key)
        self.used_bytes += sys.getsizeof(value)
        if not self.fast_combiner:
            key = self.__defensive_copy(key)
            value = self.__defensive_copy(value)
        self.data.setdefault(key, []).append(value)
        self.ctx.increment_counter(self.in_rec_counter, 1)
        if self.used_bytes >= self.spill_bytes:
            self.spill_all()

    def close(self):
        self.spill_all()

    def spill_all(self):
        self.ctx.increment_counter(self.spill_counter, 1)
        self.ctx.increment_counter(self.spilled_bytes_counter, self.used_bytes)
        ctx = self.ctx
        writer = ctx.writer
        ctx.writer = None
        # disable auto-deserialize (ctx.key will be called by reduce)
        # FIXME: this might break custom Context implementations
        get_input_key = ctx.get_input_key
        ctx.get_input_key = types.MethodType(lambda self: self._key, ctx)
        for key, values in iteritems(self.data):
            ctx._key, ctx._values = key, iter(values)
            self.reducer.reduce(ctx)
        ctx.writer = writer
        ctx.get_input_key = get_input_key
        self.data.clear()
        self.used_bytes = 0


class TaskContext(api.MapContext, api.ReduceContext):

    JOB_OUTPUT_DIR = "mapreduce.output.fileoutputformat.outputdir"
    TASK_OUTPUT_DIR = "mapreduce.task.output.dir"
    TASK_PARTITION = "mapreduce.task.partition"

    def deserializing(self, meth, deserializer):
        """
        Decorate a key/value getter to make it auto-deserialize records.
        """
        def deserialize(*args, **kwargs):
            ret = meth(*args, **kwargs)
            return deserializer.deserialize(ret)
        return deserialize

    def __init__(self, up_link, private_encoding=True, fast_combiner=False):
        self._fast_combiner = fast_combiner
        self.private_encoding = private_encoding
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
        # None = unknown (yet), e.g., while setting conf.  In this
        # case, *both* is_mapper() and is_reducer() must return False
        self._is_mapper = None

    def set_is_mapper(self):
        self._is_mapper = True

    def set_is_reducer(self):
        self._is_mapper = False

    def is_mapper(self):
        return self._is_mapper is True

    def is_reducer(self):
        return self._is_mapper is False

    def enable_private_encoding(self):
        self._private_encoding = self.private_encoding

    def close(self):
        if self.writer:
            self.writer.close()
        self.up_link.send(self.up_link.DONE)

    def set_combiner(self, factory, input_split, n_reduces):
        self.n_reduces = n_reduces
        if self.n_reduces > 0:
            self.partitioner = factory.create_partitioner(self)
            reducer = factory.create_combiner(self)
            spill_size = self._job_conf.get_int(
                "mapreduce.task.io.sort.mb", DEFAULT_IO_SORT_MB
            )
            if reducer:
                self.writer = CombineRunner(spill_size * 1024 * 1024,
                                            self, reducer,
                                            fast_combiner=self._fast_combiner)
            else:
                self.writer = None

    def emit(self, key, value):
        self.progress()
        if self.writer:
            self.writer.emit(key, value)
        else:
            if self._is_mapper and self._private_encoding:
                key = private_encode(key)
                value = private_encode(value)
            if self.partitioner:
                part = self.partitioner.partition(key, self.n_reduces)
                self.up_link.send(self.up_link.PARTITIONED_OUTPUT,
                                  part, key, value)
            else:
                self.up_link.send(self.up_link.OUTPUT, key, value)

    def set_job_conf(self, vals):
        self._job_conf = api.JobConf(vals)

    # FIXME: currently works only with the default TextInputFormat;
    # TODO: generalize to support Hadoop Writable types
    def setup_deserialization(self):
        """\
        Set up auto-deserialization of input key/values
        """
        # NOTE: assuming up link is binary => down link is binary
        # the dict check is for the simulator
        if isinstance(self.up_link, (BinaryUpStreamAdapter, dict)):
            if not _INPUT_FORMAT_KEYS.intersection(self._job_conf):
                self.get_input_key = self.deserializing(
                    self.get_input_key, LongWritableDeserializer()
                )
                self.get_input_value = self.deserializing(
                    self.get_input_value, TextDeserializer()
                )

    def setup_serialization(self):
        """\
        Set up auto-serialization of output key/values
        """
        pass

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
            return
        now = int(time.time())
        if now - self._last_progress > 1:
            self._last_progress = now
            if self._status_set:
                self.up_link.send(self.up_link.STATUS, self._status)
                LOGGER.debug("Sending status: %r", self._status)
                self._status_set = False
            self.up_link.send(self.up_link.PROGRESS, self._progress_float)
            self.up_link.flush()
            LOGGER.debug("Sending progress: %r", self._progress_float)

    def set_status(self, status):
        self._status = status
        self._status_set = True
        self.progress()

    def get_counter(self, group, name):
        counter_id = len(self._registered_counters)
        self._registered_counters.append(counter_id)
        self.up_link.send(self.up_link.REGISTER_COUNTER,
                          counter_id, group, name)
        return api.Counter(counter_id)

    def increment_counter(self, counter, amount):
        self.up_link.send(self.up_link.INCREMENT_COUNTER,
                          counter.get_id(), amount)

    def get_input_split(self, raw=False):
        return self._input_split if raw else InputSplit(self._input_split)

    def get_input_key_class(self):
        return self._input_key_class

    def get_input_value_class(self):
        return self._input_value_class

    def next_value(self):
        try:
            self._value = next(self._values)
            return True
        except StopIteration:
            return False

    def get_output_dir(self):
        return self._job_conf[self.JOB_OUTPUT_DIR]

    def get_work_path(self):
        try:
            return self._job_conf[self.TASK_OUTPUT_DIR]
        except KeyError:
            raise RuntimeError("%r not set" % (self.TASK_OUTPUT_DIR,))

    def get_task_partition(self):
        return self._job_conf.get_int(self.TASK_PARTITION)

    def get_default_work_file(self, extension=""):
        partition = self.get_task_partition()
        if partition is None:
            raise RuntimeError("%r not set" % (self.TASK_PARTITION,))
        base = self._job_conf.get("mapreduce.output.basename", "part")
        task_type = "r" if self.is_reducer() else "m"
        return "%s/%s-%s-%05d%s" % (
            self.get_work_path(), base, task_type, partition, extension
        )


def resolve_connections(port=None, istream=None, ostream=None, cmd_file=None,
                        auto_serialize=True):
    """
    Select appropriate connection streams and protocol.
    """
    port = port or get_command_port()
    cmd_file = cmd_file or get_command_file()
    if port is not None:
        port = int(port)
        conn = connections.open_network_connections(port, auto_serialize)
    elif cmd_file is not None:
        out_file = cmd_file + '.out'
        conn = connections.open_playback_connections(
            cmd_file, out_file, auto_serialize
        )
    else:
        # auto_serialize has no effect here. Should we warn the user?
        istream = sys.stdin if istream is None else istream
        ostream = sys.stdout if ostream is None else ostream
        conn = connections.open_file_connections(istream=istream,
                                                 ostream=ostream)
    return conn


class StreamRunner(object):

    def __init__(self, factory, context, cmd_stream):
        self.logger = LOGGER.getChild('StreamRunner')
        self.factory = factory
        self.ctx = context
        self.cmd_stream = cmd_stream
        self.password = None
        self.authenticated = False
        self.get_password()

    def get_password(self):
        pfile_name = get_secret_location()
        self.logger.debug('secret location: %r', pfile_name)
        if pfile_name is None:
            self.password = None
            return
        try:
            with open(pfile_name, 'rb') as f:
                self.password = f.read()
                self.logger.debug('password: %r', self.password)
        except IOError:
            self.logger.error('Could not open the password file')

    def run(self):
        self.logger.debug('start running')
        AUTHENTICATION_REQ = self.cmd_stream.AUTHENTICATION_REQ
        SET_JOB_CONF = self.cmd_stream.SET_JOB_CONF
        RUN_MAP = self.cmd_stream.RUN_MAP
        RUN_REDUCE = self.cmd_stream.RUN_REDUCE

        for cmd, args in self.cmd_stream:
            self.logger.debug('dispatching cmd: %s, args: %s', cmd, args)
            if cmd == AUTHENTICATION_REQ:
                digest, challenge = args
                self.logger.debug(
                    'authenticationReq: %r, %r', digest, challenge)
                if self.fails_to_authenticate(digest, challenge):
                    self.logger.critical('Server failed to authenticate')
                    break  # bailing out
            elif cmd == SET_JOB_CONF:
                self.ctx.set_job_conf(args[0])
            elif cmd == RUN_MAP:
                self.ctx.set_is_mapper()
                input_split, n_reduces, piped_input = args
                if piped_input:
                    self.ctx.setup_deserialization()
                self.run_map(input_split, n_reduces, piped_input)
                break  # we can bail out, there is nothing more to do.
            elif cmd == RUN_REDUCE:
                self.ctx.set_is_reducer()
                part, piped_output = args
                if piped_output:
                    self.ctx.setup_serialization()
                self.run_reduce(part, piped_output)
                break  # we can bail out, there is nothing more to do.
        self.logger.debug('done')

    def fails_to_authenticate(self, digest, challenge):
        if self.password is None:
            self.logger.info('No password, assuming playback mode')
            self.authenticated = True
            return False
        expected_digest = create_digest(self.password, challenge)
        if expected_digest != digest:
            return True
        self.authenticated = True
        response_digest = create_digest(self.password, digest)
        self.logger.debug('authenticationResp: %r', response_digest)
        self.ctx.up_link.send(self.ctx.up_link.AUTHENTICATION_RESP,
                              response_digest)
        self.ctx.up_link.flush()
        return False

    def run_map(self, input_split, n_reduces, piped_input):
        self.logger.debug('start run_map')
        factory, ctx = self.factory, self.ctx
        if n_reduces > 0:
            ctx.enable_private_encoding()
        ctx._input_split = input_split
        LOGGER.debug("Input split: %r", input_split)
        if piped_input:
            cmd, args = next(iter(self.cmd_stream))
            if cmd == self.cmd_stream.SET_INPUT_TYPES:
                ctx._input_key_class, ctx._input_value_class = args
                LOGGER.debug("Input (key, value) class: (%r, %r)",
                             ctx._input_key_class, ctx._input_value_class)
        reader = factory.create_record_reader(ctx)
        if reader is None and not piped_input:
            raise api.PydoopError('RecordReader not defined')
        send_progress = reader is not None
        mapper = factory.create_mapper(ctx)
        reader = reader if reader else get_key_value_stream(self.cmd_stream)
        ctx.set_combiner(factory, input_split, n_reduces)
        mapper_map = mapper.map
        progress_function = ctx.progress
        if n_reduces == 0:
            ctx.writer = factory.create_record_writer(ctx)
            if ctx.writer is None and not ctx.job_conf.get_bool(IS_JAVA_RW):
                raise api.PydoopError('RecordWriter not defined')
        for ctx._key, ctx._value in reader:
            if send_progress:
                ctx._progress_float = reader.get_progress()
                LOGGER.debug("Progress updated to %r ", ctx._progress_float)
                progress_function()
            mapper_map(ctx)
        mapper.close()
        self.logger.debug('done with run_map')

    def run_reduce(self, part, piped_output):
        self.logger.debug('start run_reduce')
        factory, ctx = self.factory, self.ctx
        writer = factory.create_record_writer(ctx)
        if writer is None and piped_output is None:
            raise api.PydoopError('RecordWriter not defined')
        ctx.writer = writer
        reducer = factory.create_reducer(ctx)
        kvs_stream = get_key_values_stream(self.cmd_stream,
                                           ctx.private_encoding)
        reducer_reduce = reducer.reduce
        for ctx._key, ctx._values in kvs_stream:
            reducer_reduce(ctx)
        reducer.close()
        self.logger.debug('done with run_reduce')


def run_task(factory, port=None, istream=None, ostream=None,
             private_encoding=True, context_class=TaskContext,
             cmd_file=None, fast_combiner=False, auto_serialize=True):
    """
    Run the assigned task in the framework.

    :rtype: bool
    :return: :obj:`True` if the task succeeded.
    """
    connections = resolve_connections(
        port, istream=istream, ostream=ostream, cmd_file=cmd_file,
        auto_serialize=auto_serialize
    )
    context = context_class(connections.up_link,
                            private_encoding=private_encoding,
                            fast_combiner=fast_combiner)
    stream_runner = StreamRunner(factory, context, connections.cmd_stream)
    pstats_dir = os.getenv(PSTATS_DIR)
    if pstats_dir:
        pstats_fmt = os.getenv(PSTATS_FMT, DEFAULT_PSTATS_FMT)
        hdfs.mkdir(pstats_dir)
        fd, pstats_fn = tempfile.mkstemp(suffix=".pstats")
        os.close(fd)
        cProfile.runctx("stream_runner.run()",
                        {"stream_runner": stream_runner}, globals(),
                        filename=pstats_fn)
        name = pstats_fmt % (
            "r" if context.is_reducer() else "m",
            context.get_task_partition(), os.path.basename(pstats_fn)
        )
        hdfs.put(pstats_fn, hdfs.path.join(pstats_dir, name))
    else:
        stream_runner.run()
    context.close()
    connections.close()
    return True
