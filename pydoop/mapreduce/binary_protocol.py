# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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

"""\
Client side of the Hadoop pipes protocol.

Ref: ``org.apache.hadoop.mapred.pipes.BinaryProtocol``.
"""

import os
try:
    from cPickle import loads
except ImportError:
    from pickle import loads
from itertools import groupby
from operator import itemgetter

import pydoop.config as config
from .api import AVRO_IO_MODES, JobConf


PROTOCOL_VERSION = 0

# We can use an enum.IntEnum after dropping Python2 compatibility
START = 0
SET_JOB_CONF = 1
SET_INPUT_TYPES = 2
RUN_MAP = 3
MAP_ITEM = 4
RUN_REDUCE = 5
REDUCE_KEY = 6
REDUCE_VALUE = 7
CLOSE = 8
ABORT = 9
AUTHENTICATION_REQ = 10
OUTPUT = 50
PARTITIONED_OUTPUT = 51
STATUS = 52
PROGRESS = 53
DONE = 54
REGISTER_COUNTER = 55
INCREMENT_COUNTER = 56
AUTHENTICATION_RESP = 57

CMD_REPR = {
    START: "START",
    SET_JOB_CONF: "SET_JOB_CONF",
    SET_INPUT_TYPES: "SET_INPUT_TYPES",
    RUN_MAP: "RUN_MAP",
    MAP_ITEM: "MAP_ITEM",
    RUN_REDUCE: "RUN_REDUCE",
    REDUCE_KEY: "REDUCE_KEY",
    REDUCE_VALUE: "REDUCE_VALUE",
    CLOSE: "CLOSE",
    ABORT: "ABORT",
    AUTHENTICATION_REQ: "AUTHENTICATION_REQ",
    OUTPUT: "OUTPUT",
    PARTITIONED_OUTPUT: "PARTITIONED_OUTPUT",
    STATUS: "STATUS",
    PROGRESS: "PROGRESS",
    DONE: "DONE",
    REGISTER_COUNTER: "REGISTER_COUNTER",
    INCREMENT_COUNTER: "INCREMENT_COUNTER",
    AUTHENTICATION_RESP: "AUTHENTICATION_RESP",
}

IS_JAVA_RW = "mapreduce.pipes.isjavarecordwriter"


def get_password():
    try:
        pass_fn = os.environ["hadoop.pipes.shared.secret.location"]
    except KeyError:
        return None
    with open(pass_fn, "rb") as f:
        return f.read()


# _get_* functions to patch the downlink according to the chosen
# deserialization policy (see below)

def _get_LongWritable(downlink):
    assert downlink.stream.read_vint() == 8
    return downlink.stream.read_long_writable()


def _get_Text(downlink):
    return downlink.stream.read_string()


DESERIALIZERS = {
    "org.apache.hadoop.io.LongWritable": _get_LongWritable,
    "org.apache.hadoop.io.Text": _get_Text,
}


def _get_avro_key(downlink):
    raw = downlink.stream.read_bytes()
    return downlink.avro_key_deserializer.deserialize(raw)


def _get_avro_value(downlink):
    raw = downlink.stream.read_bytes()
    return downlink.avro_value_deserializer.deserialize(raw)


def _get_pickled(downlink):
    return loads(downlink.stream.read_bytes())


class Downlink(object):
    """\
    Reads and executes pipes commands as directed by upstream.

    The downlink drives the entire MapReduce task, plugging in user components
    and calling their methods as necessary. A task can be either a **map**
    task or a **reduce** task, but this is not known until after a few initial
    commands, as shown below.

    All tasks start with the following commands::

        AUTHENTICATION_REQ
        START
        SET_JOB_CONF

    Map tasks follow up with::

        RUN_MAP
        if java_reader:
            SET_INPUT_TYPES
            for k, v in input:
                MAP_ITEM
            CLOSE

    Reduce tasks follow up with::

        RUN_REDUCE
        for k in input:
            REDUCE_KEY
            for v in values_for(k):
                REDUCE_VALUE
        CLOSE

    In both cases, the inner loop consists of handling the key/value
    stream. All the code involved in this process, namely:

      * reading and optionally deserializing input keys and values
      * calling user methods
      * emitting output keys and values back to upstream

    must be as efficient as possible. For this reason, rather than having the
    ``get_{k,v}`` methods go through a complex ``if`` tree at every call, we
    patch the class itself by replacing each method with the one appropriate
    for the current scenario. Note that we can do this because:

      * the deserialization policy (including no deserialization) is the same
        for all items of a given kind (key or value), meaning that an ``if``
        tree would pick the same branch for the entire process
      * there is only one Downlink object in the process, so we don't risk
        altering the behavior of other instances
      * the Downlink object is not part of the client API (it's not passed to
        user code at all)

    Job conf deserialization also needs to be somewhat efficient, since it
    involves reading thousands of strings.
    """

    def __init__(self, istream, context, **kwargs):
        self.stream = istream
        self.context = context
        self.raw_k = kwargs.get("raw_keys", False)
        self.raw_v = kwargs.get("raw_values", False)
        self.password = get_password()
        self.auth_done = False
        self.avro_key_deserializer = None
        self.avro_value_deserializer = None

    def close(self):
        self.stream.close()

    def read_job_conf(self):
        n = self.stream.read_vint()
        if n & 1:
            raise RuntimeError("number of items is not even")
        t = self.stream.read_tuple(n * 's')
        return JobConf(t[i: i + 2] for i in range(0, n, 2))

    def verify_digest(self, digest, challenge):
        if self.password is not None:
            self.context._authenticate(self.password, digest, challenge)
        # self.password is None: assume reading from cmd file
        self.auth_done = True

    def setup_record_writer(self, piped_output):
        writer = self.context.create_record_writer()
        if writer and piped_output:
            raise RuntimeError("record writer defined when not needed")
        if not writer and not piped_output:
            raise RuntimeError("record writer not defined")

    def get_k(self):
        return self.stream.read_bytes()

    def get_v(self):
        return self.stream.read_bytes()

    def setup_avro_deser(self):
        try:
            from pydoop.avrolib import AvroDeserializer
        except ImportError as e:
            raise RuntimeError("cannot handle avro input: %s" % e)
        jc = self.context.job_conf
        avro_input = jc.get(config.AVRO_INPUT).upper()
        if avro_input not in AVRO_IO_MODES:
            raise RuntimeError('invalid avro input mode: %s' % avro_input)
        if avro_input == 'K' or avro_input == 'KV' and not self.raw_k:
            schema = jc.get(config.AVRO_KEY_INPUT_SCHEMA)
            self.avro_key_deserializer = AvroDeserializer(schema)
            self.__class__.get_k = _get_avro_key
        if avro_input == 'V' or avro_input == 'KV' and not self.raw_v:
            schema = jc.get(config.AVRO_VALUE_INPUT_SCHEMA)
            self.avro_value_deserializer = AvroDeserializer(schema)
            self.__class__.get_v = _get_avro_value

    def setup_deser(self, key_type, value_type):
        if not self.raw_k:
            d = DESERIALIZERS.get(key_type)
            if d is not None:
                self.__class__.get_k = d
        if not self.raw_v:
            d = DESERIALIZERS.get(value_type)
            if d is not None:
                self.__class__.get_v = d

    def __next__(self):
        cmd = self.stream.read_vint()
        if cmd != AUTHENTICATION_REQ and not self.auth_done:
            raise RuntimeError("%d received before authentication" % cmd)
        if cmd == AUTHENTICATION_REQ:
            digest, challenge = self.stream.read_tuple('bb')
            self.verify_digest(digest, challenge)
        elif cmd == START:
            v = self.stream.read_vint()
            if (v != PROTOCOL_VERSION):
                raise RuntimeError("Unknown protocol id: %d" % v)
        elif cmd == SET_JOB_CONF:
            self.context._job_conf = self.read_job_conf()
            if config.AVRO_OUTPUT in self.context.job_conf:
                self.context._setup_avro_ser()
        elif cmd == RUN_MAP:
            self.context.task_type = "m"
            split, nred, piped_input = self.stream.read_tuple('bii')
            self.context._raw_split = split
            reader = self.context.create_record_reader()
            if reader and piped_input:
                raise RuntimeError("record reader defined when not needed")
            if not reader and not piped_input:
                raise RuntimeError("record reader not defined")
            combiner = self.context.create_combiner()
            if nred < 1:  # map-only job
                if combiner:
                    raise RuntimeError("combiner defined in map-only job")
                self.context._private_encoding = False
                piped_output = self.context.job_conf.get_bool(IS_JAVA_RW)
                self.setup_record_writer(piped_output)
            self.context.nred = nred
            self.context.create_mapper()
            self.context.create_partitioner()
            if reader:
                for self.context._key, self.context._value in reader:
                    self.context.mapper.map(self.context)
                    self.context.progress_value = reader.get_progress()
                    self.context.progress()
                # no more commands from upstream, not even CLOSE
                try:
                    self.context.close()
                finally:
                    raise StopIteration
        elif cmd == SET_INPUT_TYPES:
            key_type, value_type = self.stream.read_tuple('ss')
            if config.AVRO_INPUT in self.context.job_conf:
                self.setup_avro_deser()
            else:
                self.setup_deser(key_type, value_type)
        elif cmd == MAP_ITEM:
            self.context._key = self.get_k()
            self.context._value = self.get_v()
            self.context.mapper.map(self.context)
        elif cmd == RUN_REDUCE:
            self.context.task_type = "r"
            part, piped_output = self.stream.read_tuple('ii')
            # for some reason, part is always 0
            self.context.create_reducer()
            self.setup_record_writer(piped_output)
            if self.context._private_encoding:
                self.__class__.get_k = _get_pickled
                self.__class__.get_v = _get_pickled
            for cmd, subs in groupby(self, itemgetter(0)):
                if cmd == REDUCE_KEY:
                    _, self.context._key = next(subs)
                if cmd == REDUCE_VALUE:
                    self.context._values = (v for _, v in subs)
                    self.context.reducer.reduce(self.context)
                if cmd == CLOSE:
                    try:
                        self.context.close()
                    finally:
                        raise StopIteration
        elif cmd == REDUCE_KEY:
            k = self.get_k()
            return cmd, k  # pass on to RUN_REDUCE iterator
        elif cmd == REDUCE_VALUE:
            v = self.get_v()
            return cmd, v  # pass on to RUN_REDUCE iterator
        elif cmd == ABORT:
            raise RuntimeError("received ABORT command")
        elif cmd == CLOSE:
            if self.context.mapper:
                try:
                    self.context.close()
                finally:
                    raise StopIteration
            else:
                return cmd, None  # pass on to RUN_REDUCE iterator
        else:
            raise RuntimeError("unknown command: %d" % cmd)

    def __iter__(self):
        return self

    # py2 compat
    def next(self):
        return self.__next__()


class Uplink(object):
    """\
    Writes all information that needs to be sent upstream.
    """

    def __init__(self, stream):
        self.stream = stream

    def flush(self):
        self.stream.flush()

    def close(self):
        self.stream.close()

    # pipes commands

    def authenticate(self, response_digest):
        self.stream.write_tuple("ib", (AUTHENTICATION_RESP, response_digest))

    def output(self, k, v):
        self.stream.write_output(k, v)

    def partitioned_output(self, part, k, v):
        self.stream.write_output(k, v, part)

    def status(self, msg):
        self.stream.write_tuple("is", (STATUS, msg))

    def progress(self, p):
        self.stream.write_tuple("if", (PROGRESS, p))

    def done(self):
        self.stream.write_vint(DONE)

    def register_counter(self, id, group, name):
        self.stream.write_tuple("iiss", (REGISTER_COUNTER, id, group, name))

    def increment_counter(self, id, amount):
        self.stream.write_tuple("iil", (INCREMENT_COUNTER, id, amount))
