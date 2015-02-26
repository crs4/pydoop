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
Avro tools.
"""
# DEV NOTE: since Avro is not a requirement, do *not* import this
# module anywhere in the main code (importing it in the Avro examples
# is OK, ofc).

import logging
logging.basicConfig()
LOGGER = logging.getLogger('avrolib')
LOGGER.setLevel(logging.DEBUG)
from cStringIO import StringIO

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter, BinaryDecoder, BinaryEncoder

import pydoop
import pydoop.mapreduce.pipes as pp
from pydoop.mapreduce.api import RecordWriter, RecordReader
import pydoop.hdfs as hdfs


AVRO_INPUT = pydoop.PROPERTIES['AVRO_INPUT']
AVRO_OUTPUT = pydoop.PROPERTIES['AVRO_OUTPUT']
AVRO_KEY_INPUT_SCHEMA = pydoop.PROPERTIES['AVRO_KEY_INPUT_SCHEMA']
AVRO_KEY_OUTPUT_SCHEMA = pydoop.PROPERTIES['AVRO_KEY_OUTPUT_SCHEMA']
AVRO_VALUE_INPUT_SCHEMA = pydoop.PROPERTIES['AVRO_VALUE_INPUT_SCHEMA']
AVRO_VALUE_OUTPUT_SCHEMA = pydoop.PROPERTIES['AVRO_VALUE_OUTPUT_SCHEMA']


class AvroContext(pp.TaskContext):
    """
    A specialized context class that allows mappers and reducers to
    work with Avro records.

    For now, this works only with the Hadoop 2 mapreduce pipes code
    (``src/v2/it/crs4/pydoop/mapreduce/pipes``).  Avro I/O mode must
    be explicitly requested when launching the application with pydoop
    submit (``--avro-input``, ``--avro-output``).
    """
    @staticmethod
    def deserializing(meth):
        """
        Decorate a key/value getter to make it auto-deserialize Avro
        records.
        """
        def with_deserialization(self, *args, **kwargs):
            ret = meth(self, *args, **kwargs)
            f = StringIO(ret)
            dec = BinaryDecoder(f)
            return self._datum_reader.read(dec)
        return with_deserialization

    def __init__(self, up_link, private_encoding=True):
        super(AvroContext, self).__init__(up_link, private_encoding)
        self._datum_reader = None
        self._datum_writer = None

    def set_job_conf(self, vals):
        """
        Set job conf and Avro datum reader/writer.
        """
        super(AvroContext, self).set_job_conf(vals)
        jc = self.get_job_conf()
        if AVRO_INPUT in jc:
            avro_input = jc.get(AVRO_INPUT).upper()
            if avro_input == 'K':
                schema_str = jc.get(AVRO_KEY_INPUT_SCHEMA)
                AvroContext.get_input_key = AvroContext.deserializing(
                    AvroContext.get_input_key
                )
            elif avro_input == 'V':
                schema_str = jc.get(AVRO_VALUE_INPUT_SCHEMA)
                AvroContext.get_input_value = AvroContext.deserializing(
                    AvroContext.get_input_value
                )
            elif avro_input == 'KV':
                raise NotImplementedError  # FIXME: TBD
            self._datum_reader = DatumReader(avro.schema.parse(schema_str))
        if AVRO_OUTPUT in jc:
            avro_output = jc.get(AVRO_OUTPUT).upper()
            if avro_output == 'K':
                schema_str = jc.get(AVRO_KEY_OUTPUT_SCHEMA)
            elif avro_output == 'V':
                schema_str = jc.get(AVRO_VALUE_OUTPUT_SCHEMA)
            elif avro_output == 'KV':
                raise NotImplementedError  # FIXME: TBD
            self._datum_writer = DatumWriter(avro.schema.parse(schema_str))

    def emit(self, key, value):
        """Emit key and value, serializing Avro data as needed.

        We need to perform Avro serialization if:

        #. AVRO_OUTPUT is in the job conf and
        #. we are either in a reducer or in a map-only app's mapper
        """
        if self.__serialize_avro():
            f = StringIO()
            encoder = BinaryEncoder(f)
            self._datum_writer.write(value, encoder)
            v = f.getvalue()
        else:
            v = value
        super(AvroContext, self).emit(key, v)

    def __serialize_avro(self):
        jc = self.job_conf
        if AVRO_OUTPUT not in jc:
            return False
        assert jc.get(AVRO_OUTPUT).upper() == 'V'  # FIXME
        if self.is_reducer():
            return True
        return jc.get_int(
            'mapred.reduce.tasks', jc.get_int('mapreduce.job.reduces', 0)
        ) < 1


class SeekableDataFileReader(DataFileReader):

    FORWARD_WINDOW_SIZE = 8192

    def align_after(self, offset):
        """
        Search for a sync point after offset and align just after that.
        """
        f = self.reader
        if offset <= 0:  # FIXME what is a negative offset??
            f.seek(0)
            self.block_count = 0
            self._read_header()  # FIXME we can't extimate how big it is...
            return
        sm = self.sync_marker
        sml = len(sm)
        pos = offset
        while pos < self.file_length - sml:
            f.seek(pos)
            data = f.read(self.FORWARD_WINDOW_SIZE)
            sync_offset = data.find(sm)
            if sync_offset > -1:
                f.seek(pos + sync_offset)
                self.block_count = 0
                return
            pos += len(data)


#FIXME this is just an example with no error checking
class AvroReader(RecordReader):
    """
    Avro data file reader.

    Reads all data blocks that begin within the given input split.
    """
    def __init__(self, ctx):
        super(AvroReader, self).__init__(ctx)
        self.logger = LOGGER.getChild('AvroReader')
        isplit = ctx.input_split
        self.region_start = isplit.offset
        self.region_end = isplit.offset + isplit.length
        self.reader = SeekableDataFileReader(hdfs.open(isplit.filename),
                                             DatumReader())
        self.reader.align_after(isplit.offset)

    def next(self):
        pos = self.reader.reader.tell()
        if pos > self.region_end and self.reader.block_count == 0:
            raise StopIteration
        record = self.reader.next()
        return pos, record

    def get_progress(self):
        """
        Give a rough estimate of the progress done.
        """
        pos = self.reader.reader.tell()
        return min((pos - self.region_start)
                   / float(self.region_end - self.region_start),
                   1.0)


#FIXME this is just an example with no error checking
class AvroWriter(RecordWriter):

    schema = None

    def __init__(self, context):
        super(AvroWriter, self).__init__(context)
        self.logger = LOGGER.getChild('AvroWriter')
        job_conf = context.job_conf
        part = int(job_conf['mapreduce.task.partition'])
        outdir = job_conf["mapreduce.task.output.dir"]
        outfn = "%s/part-%05d" % (outdir, part)
        wh = hdfs.open(outfn, "w")
        self.logger.debug('created hdfs file %s', outfn)
        self.writer = DataFileWriter(wh, DatumWriter(), self.schema)
        self.logger.debug('opened AvroWriter')

    def close(self):
        self.writer.close()
        # FIXME do we really need to explicitely close the filesystem?
        self.writer.writer.fs.close()
        self.logger.debug('closed AvroWriter')
