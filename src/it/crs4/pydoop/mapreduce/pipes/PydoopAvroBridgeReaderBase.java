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

import java.io.IOException;
import java.io.ByteArrayOutputStream;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.io.Text;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.BinaryEncoder;


public abstract class PydoopAvroBridgeReaderBase<K, V>
    extends RecordReader<K, V> {

  private static final String COUNTERS_GROUP =
    PydoopAvroBridgeReaderBase.class.getName();

  protected RecordReader actualReader;
  protected List<Schema> schemas;
  protected List<Text> outRecords;
  protected List<DatumWriter<IndexedRecord>> datumWriters;
  protected List<BinaryEncoder> encoders;
  protected List<ByteArrayOutputStream> outStreams;

  protected Counter nRecords;
  protected Counter readTimeCounter;
  protected Counter serTimeCounter;

  private List<IndexedRecord> bufferedInRecords;
  private long start;
  private boolean hasRecord;

  /**
   * Get current record(s) from the actual (input) RecordReader.
   * The returned list should contain one element for key-only or
   * value-only readers, two for key/value readers (this is not
   * enforced here, however).  This method must NOT advance the actual
   * reader (it's the equivalent of getCurrent{Key,Value}, not of
   * nextKeyValue).
   */
  protected abstract List<IndexedRecord> getInRecords()
      throws IOException, InterruptedException;

  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    actualReader.initialize(split, context);
    nRecords = context.getCounter(COUNTERS_GROUP, "Number of records");
    readTimeCounter = context.getCounter(COUNTERS_GROUP, "Read time (ms)");
    serTimeCounter = context.getCounter(
        COUNTERS_GROUP, "Serialization time (ms)");
    // peek at the record stream and save the schema(s) so that the concrete
    // subclass can set the schema property during initialization
    start = System.nanoTime();
    hasRecord = actualReader.nextKeyValue();
    if (hasRecord) {
      readTimeCounter.increment((System.nanoTime() - start) / 1000000);
      bufferedInRecords = getInRecords();
      schemas = new ArrayList<Schema>();
      datumWriters = new ArrayList<DatumWriter<IndexedRecord>>();
      outStreams = new ArrayList<ByteArrayOutputStream>();
      encoders = new ArrayList<BinaryEncoder>();
      outRecords = new ArrayList<Text>();
      for (IndexedRecord r: bufferedInRecords) {
        Schema s = r.getSchema();
        schemas.add(s);
        datumWriters.add(new GenericDatumWriter<IndexedRecord>(s));
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        outStreams.add(stream);
        encoders.add(EncoderFactory.get().binaryEncoder(stream, null));
        outRecords.add(new Text());
      }
    }
  }

  public synchronized boolean nextKeyValue()
      throws IOException, InterruptedException {
    List<IndexedRecord> records = null;
    if (bufferedInRecords == null) {
      start = System.nanoTime();
      hasRecord = actualReader.nextKeyValue();
      if (!hasRecord) {
        return false;
      }
      else {
        readTimeCounter.increment((System.nanoTime() - start) / 1000000);
        records = getInRecords();
      }
    }
    else {
      records = bufferedInRecords;
      bufferedInRecords = null;
    }
    //--
    Iterator<IndexedRecord> iterRecords = records.iterator();
    Iterator<DatumWriter<IndexedRecord>> iterWriters = datumWriters.iterator();
    Iterator<BinaryEncoder> iterEncoders = encoders.iterator();
    Iterator<ByteArrayOutputStream> iterStreams = outStreams.iterator();
    Iterator<Text> iterOutRecords = outRecords.iterator();
    start = System.nanoTime();
    while (iterRecords.hasNext()) {
      ByteArrayOutputStream stream = iterStreams.next();
      BinaryEncoder enc = iterEncoders.next();
      try {
        iterWriters.next().write(iterRecords.next(), enc);
        enc.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      iterOutRecords.next().set(new Text(stream.toByteArray()));
      stream.reset();
    }
    serTimeCounter.increment((System.nanoTime() - start) / 1000000);
    nRecords.increment(1);
    return true;
  }

  public float getProgress() throws IOException,  InterruptedException {
    return actualReader.getProgress();
  }

  public synchronized void close() throws IOException {
    actualReader.close();
  }

}
