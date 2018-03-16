// BEGIN_COPYRIGHT
//
// Copyright 2009-2018 CRS4.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
//
// END_COPYRIGHT

package it.crs4.pydoop.mapreduce.pipes;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.BinaryDecoder;

import static it.crs4.pydoop.mapreduce.pipes.Submitter.AvroIO;


public abstract class PydoopAvroBridgeWriterBase
    extends RecordWriter<Text, Text> {

  private static final String COUNTERS_GROUP =
    PydoopAvroBridgeWriterBase.class.getName();
  private long start;

  protected AvroIO mode;
  protected RecordWriter actualWriter;
  protected DecoderFactory decFactory;
  protected List<DatumReader<GenericRecord>> datumReaders;
  protected List<Decoder> decoders;
  protected List<GenericRecord> outRecords;

  protected Counter nRecords;
  protected Counter writeTimeCounter;
  protected Counter deserTimeCounter;

  public PydoopAvroBridgeWriterBase(TaskAttemptContext context, AvroIO mode) {
    Properties props = Submitter.getPydoopProperties();
    Configuration conf = context.getConfiguration();
    datumReaders = new ArrayList<DatumReader<GenericRecord>>();
    decoders = new ArrayList<Decoder>();
    outRecords = new ArrayList<GenericRecord>();
    if (mode == AvroIO.K || mode == AvroIO.KV) {
      datumReaders.add(new GenericDatumReader<GenericRecord>(Schema.parse(
          conf.get(props.getProperty("AVRO_KEY_OUTPUT_SCHEMA")))));
      decoders.add(null);
      outRecords.add(null);
    }
    if (mode == AvroIO.V || mode == AvroIO.KV) {
      datumReaders.add(new GenericDatumReader<GenericRecord>(Schema.parse(
          conf.get(props.getProperty("AVRO_VALUE_OUTPUT_SCHEMA")))));
      decoders.add(null);
      outRecords.add(null);
    }
    decFactory = DecoderFactory.get();
    this.mode = mode;
    //--
    nRecords = context.getCounter(COUNTERS_GROUP, "Number of records");
    writeTimeCounter = context.getCounter(COUNTERS_GROUP, "Write time (ms)");
    deserTimeCounter = context.getCounter(
        COUNTERS_GROUP, "Deserialization time (ms)");
  }

  protected List<GenericRecord> getOutRecords(List<Text> inRecords)
      throws IOException {
    start = System.nanoTime();
    for (int i = 0; i < inRecords.size(); i++) {
      Decoder dec = decFactory.binaryDecoder(
          inRecords.get(i).getBytes(), (BinaryDecoder) decoders.get(i));
      decoders.set(i, dec);
      outRecords.set(i, datumReaders.get(i).read(outRecords.get(i), dec));
    }
    deserTimeCounter.increment((System.nanoTime() - start) / 1000000);
    return outRecords;
  }

  protected void write(List<GenericRecord> outRecords)
      throws IOException, InterruptedException {
    start = System.nanoTime();
    switch (mode) {
    case K:
      actualWriter.write(outRecords.get(0), NullWritable.get());
      break;
    case V:
      // Parquet writer does not accept a NullWritable key
      GenericRecord r = outRecords.get(0);
      actualWriter.write(null, r);
      break;
    case KV:
      actualWriter.write(outRecords.get(0), outRecords.get(1));
      break;
    default:
      throw new RuntimeException("Invalid Avro I/O mode");
    }
    writeTimeCounter.increment((System.nanoTime() - start) / 1000000);
    nRecords.increment(1);
  }

  public void close(TaskAttemptContext context)
      throws IOException, InterruptedException {
    actualWriter.close(context);
  }

}
