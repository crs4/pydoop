# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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

"""\
Python driver for Hadoop Pipes tasks.

The intended usage is to import this module in the executable script passed to
``mapred pipes`` (or ``pydoop submit``) and call ``run_task`` with the
appropriate arguments (see the docs and examples for further details).
"""

import base64
import hashlib
import hmac
import io
import os
import struct

try:
    from cPickle import dumps, loads, HIGHEST_PROTOCOL
except ImportError:
    from pickle import dumps, loads, HIGHEST_PROTOCOL
from time import time
from sys import getsizeof as sizeof

import pydoop.config as config
import pydoop.sercore as sercore

from . import api, connections

# py2 compat
try:
    as_text = unicode
except NameError:
    as_text = str

PSTATS_DIR = "PYDOOP_PSTATS_DIR"
PSTATS_FMT = "PYDOOP_PSTATS_FMT"
DEFAULT_PSTATS_FMT = "%s_%05d_%s"  # task_type, task_id, random suffix

INT_WRITABLE_FMT = ">i"
INT_WRITABLE_SIZE = struct.calcsize(INT_WRITABLE_FMT)


def create_digest(key, msg):
    h = hmac.new(key, msg, hashlib.sha1)
    return base64.b64encode(h.digest())


# extra support for java types, not meant for performance-critical sections

def read_int_writable(f):
    buf = f.read(INT_WRITABLE_SIZE)
    return struct.unpack(INT_WRITABLE_FMT, buf)[0]


def write_int_writable(n, f):
    f.write(struct.pack(INT_WRITABLE_FMT, n))


def read_bytes_writable(f):
    length = read_int_writable(f)
    buf = f.read(length)
    if len(buf) < length:
        raise RuntimeError("expected %d bytes, found %d" % (length, len(buf)))
    return buf


def write_bytes_writable(s, f):
    write_int_writable(len(s), f)
    if len(s) > 0:
        f.write(s)


class FileSplit(api.FileSplit):

    @classmethod
    def frombuffer(cls, buf):
        filename, offset, length = sercore.deserialize_file_split(buf)
        return cls(filename, offset, length)


class OpaqueSplit(api.OpaqueSplit):

    @classmethod
    def frombuffer(cls, buf):
        return cls.read(io.BytesIO(buf))

    @classmethod
    def read(cls, f):
        return cls(loads(read_bytes_writable(f)))

    def write(self, f):
        write_bytes_writable(dumps(self.payload, HIGHEST_PROTOCOL), f)


def write_opaque_splits(splits, f):
    write_int_writable(len(splits), f)
    for s in splits:
        s.write(f)


def read_opaque_splits(f):
    n = read_int_writable(f)
    return [OpaqueSplit.read(f) for _ in range(n)]


class TaskContext(api.Context):

    JOB_OUTPUT_DIR = "mapreduce.output.fileoutputformat.outputdir"
    TASK_OUTPUT_DIR = "mapreduce.task.output.dir"
    TASK_PARTITION = "mapreduce.task.partition"

    def __init__(self, factory, **kwargs):
        self.factory = factory
        self.uplink = None
        self.combiner = None
        self.mapper = None
        self.partitioner = None
        self.record_reader = None
        self.record_writer = None
        self.reducer = None
        self.nred = None
        self.progress_value = 0.0
        self.last_progress_t = 0.0
        self.status = None
        self.counters = {}
        self.task_type = None
        self.avro_key_serializer = None
        self.avro_value_serializer = None
        self._private_encoding = kwargs.get("private_encoding", True)
        self._raw_split = None
        self._input_split = None
        self._job_conf = {}
        self._key = None
        self._value = None
        self._values = None
        self.__auto_serialize = kwargs.get("auto_serialize", True)
        self.__cache = {}
        self.__cache_size = 0
        self.__spill_size = None  # delayed until (if) create_combiner
        self.__spilling = True  # enable actual emit

    def get_input_split(self, raw=False):
        if raw:
            return self._raw_split
        if not self._input_split:
            if config.PIPES_EXTERNALSPLITS_URI in self._job_conf:
                self._input_split = OpaqueSplit.frombuffer(self._raw_split)
            else:
                self._input_split = FileSplit.frombuffer(self._raw_split)
        return self._input_split

    def get_job_conf(self):
        return self._job_conf

    def get_input_key(self):
        return self._key

    def get_input_value(self):
        return self._value

    def get_input_values(self):
        return self._values

    def create_combiner(self):
        self.combiner = self.factory.create_combiner(self)
        if self.combiner:
            self.__spill_size = 1024 * 1024 * self.job_conf.get_int(
                "mapreduce.task.io.sort.mb", 100
            )
            self.__spilling = False
        return self.combiner

    def create_mapper(self):
        self.mapper = self.factory.create_mapper(self)
        return self.mapper

    def create_partitioner(self):
        self.partitioner = self.factory.create_partitioner(self)
        return self.partitioner

    def create_record_reader(self):
        self.record_reader = self.factory.create_record_reader(self)
        return self.record_reader

    def create_record_writer(self):
        self.record_writer = self.factory.create_record_writer(self)
        return self.record_writer

    def create_reducer(self):
        self.reducer = self.factory.create_reducer(self)
        return self.reducer

    def progress(self):
        """\
        Report progress to the Java side.

        This needs to flush the uplink stream, but too many flushes can
        disrupt performance, so we actually talk to upstream once per second.
        """
        now = time()
        if now - self.last_progress_t > 1:
            self.last_progress_t = now
            if self.status:
                self.uplink.status(self.status)
                self.status = None
            self.__spill_counters()
            self.uplink.progress(self.progress_value)
            self.uplink.flush()

    def set_status(self, status):
        self.status = status
        self.progress()

    def get_counter(self, group, name):
        id = len(self.counters)
        self.uplink.register_counter(id, group, name)
        self.uplink.flush()
        self.counters[id] = 0
        return id

    def increment_counter(self, counter, amount):
        try:
            self.counters[counter] += amount
        except KeyError:
            raise ValueError("invalid counter: %r" % (counter,))

    def __spill_counters(self):
        for c, amount in self.counters.items():
            if amount:
                self.uplink.increment_counter(c, amount)
                self.counters[c] = 0

    def _authenticate(self, password, digest, challenge):
        if create_digest(password, challenge) != digest:
            raise RuntimeError("server failed to authenticate")
        response_digest = create_digest(password, digest)
        self.uplink.authenticate(response_digest)
        self.uplink.flush()

    def _setup_avro_ser(self):
        try:
            from pydoop.avrolib import AvroSerializer
        except ImportError as e:
            raise RuntimeError("cannot handle avro output: %s" % e)
        jc = self.job_conf
        avro_output = jc.get(config.AVRO_OUTPUT).upper()
        if avro_output not in api.AVRO_IO_MODES:
            raise RuntimeError('invalid avro output mode: %s' % avro_output)
        if avro_output == 'K' or avro_output == 'KV':
            schema = jc.get(config.AVRO_KEY_OUTPUT_SCHEMA)
            self.avro_key_serializer = AvroSerializer(schema)
        if avro_output == 'V' or avro_output == 'KV':
            schema = jc.get(config.AVRO_VALUE_OUTPUT_SCHEMA)
            self.avro_value_serializer = AvroSerializer(schema)

    def __maybe_serialize(self, key, value):
        if self.task_type == "m" and self._private_encoding:
            return dumps(key, HIGHEST_PROTOCOL), dumps(value, HIGHEST_PROTOCOL)
        if self.avro_key_serializer:
            key = self.avro_key_serializer.serialize(key)
        elif self.__auto_serialize:
            key = as_text(key).encode("utf-8")
        if self.avro_value_serializer:
            value = self.avro_value_serializer.serialize(value)
        elif self.__auto_serialize:
            value = as_text(value).encode("utf-8")
        return key, value

    def emit(self, key, value):
        """\
        Handle an output key/value pair.

        Reporting progress is strictly necessary only when using a Python
        record writer, because sending an output key/value pair is an implicit
        progress report. To take advantage of this, however, we would be
        forced to flush the uplink stream at every output, and that would be
        too costly. Rather than add a specific timer for this, we just call
        progress unconditionally and piggyback on its timer instead. Note that
        when a combiner is caching there is no actual output, so in that case
        we would need an explicit progress report anyway.
        """
        if self.__spilling:
            self.__actual_emit(key, value)
        else:
            # key must be hashable
            self.__cache.setdefault(key, []).append(value)
            self.__cache_size += sizeof(key) + sizeof(value)
            if self.__cache_size >= self.__spill_size:
                self.__spill_all()
        self.progress()

    def __actual_emit(self, key, value):
        if self.record_writer:
            self.record_writer.emit(key, value)
            return
        key, value = self.__maybe_serialize(key, value)
        if self.partitioner:
            part = self.partitioner.partition(key, self.nred)
            self.uplink.partitioned_output(part, key, value)
        else:
            self.uplink.output(key, value)

    def __spill_all(self):
        self.__spilling = True
        for k in sorted(self.__cache):
            self._key = k
            self._values = iter(self.__cache[k])
            self.combiner.reduce(self)
        self.__cache.clear()
        self.__cache_size = 0
        self.__spilling = False

    def close(self):
        self.uplink.flush()
        # do *not* call uplink.done while user components are still active
        try:
            if self.mapper:
                self.mapper.close()
            # handle combiner after mapper (mapper.close can call emit)
            if self.__cache:
                self.__spill_all()
                self.__spilling = True  # re-enable emit for combiner.close
                self.combiner.close()
            if self.record_reader:
                self.record_reader.close()
            if self.record_writer:
                self.record_writer.close()
            if self.reducer:
                self.reducer.close()
            self.__spill_counters()
        finally:
            self.uplink.done()
            self.uplink.flush()

    def get_output_dir(self):
        return self.job_conf[self.JOB_OUTPUT_DIR]

    def get_work_path(self):
        try:
            return self.job_conf[self.TASK_OUTPUT_DIR]
        except KeyError:
            raise RuntimeError("%r not set" % (self.TASK_OUTPUT_DIR,))

    def get_task_partition(self):
        return self.job_conf.get_int(self.TASK_PARTITION)

    def get_default_work_file(self, extension=""):
        partition = self.get_task_partition()
        if partition is None:
            raise RuntimeError("%r not set" % (self.TASK_PARTITION,))
        base = self.job_conf.get("mapreduce.output.basename", "part")
        return "%s/%s-%s-%05d%s" % (
            self.get_work_path(), base, self.task_type, partition, extension
        )


class Factory(api.Factory):

    def __init__(self, mapper_class,
                 reducer_class=None,
                 combiner_class=None,
                 partitioner_class=None,
                 record_writer_class=None,
                 record_reader_class=None):
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


def _run(context, **kwargs):
    with connections.get_connection(context, **kwargs) as connection:
        for _ in connection.downlink:
            pass


def run_task(factory, **kwargs):
    """\
    Run a MapReduce task.

    Available keyword arguments:

    * ``raw_keys`` (default: :obj:`False`): pass map input keys to context
      as byte strings (ignore any type information)
    * ``raw_values`` (default: :obj:`False`): pass map input values to context
      as byte strings (ignore any type information)
    * ``private_encoding`` (default: :obj:`True`): automatically serialize map
      output k/v and deserialize reduce input k/v (pickle)
    * ``auto_serialize`` (default: :obj:`True`): automatically serialize reduce
      output (map output in map-only jobs) k/v (call str/unicode then encode as
      utf-8)

    Advanced keyword arguments:

    * ``pstats_dir``: run the task with cProfile and store stats in this dir
    * ``pstats_fmt``: use this pattern for pstats filenames (experts only)

    The pstats dir and filename pattern can also be provided via ``pydoop
    submit`` arguments, with lower precedence in case of clashes.
    """
    context = TaskContext(factory, **kwargs)
    pstats_dir = kwargs.get("pstats_dir", os.getenv(PSTATS_DIR))
    if pstats_dir:
        import cProfile
        import tempfile
        import pydoop.hdfs as hdfs
        hdfs.mkdir(pstats_dir)
        fd, pstats_fn = tempfile.mkstemp(suffix=".pstats")
        os.close(fd)
        cProfile.runctx(
            "_run(context, **kwargs)", globals(), locals(),
            filename=pstats_fn
        )
        pstats_fmt = kwargs.get(
            "pstats_fmt",
            os.getenv(PSTATS_FMT, DEFAULT_PSTATS_FMT)
        )
        name = pstats_fmt % (
            context.task_type,
            context.get_task_partition(),
            os.path.basename(pstats_fn)
        )
        hdfs.put(pstats_fn, hdfs.path.join(pstats_dir, name))
    else:
        _run(context, **kwargs)
