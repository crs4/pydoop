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
      outRecords = new ArrayList<Text>();
      for (IndexedRecord r: bufferedInRecords) {
        schemas.add(r.getSchema());
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
    Iterator<Schema> iterSchemas = schemas.iterator();
    Iterator<Text> iterOutRecords = outRecords.iterator();
    start = System.nanoTime();
    while (iterRecords.hasNext()) {
      assert iterSchemas.hasNext() && iterOutRecords.hasNext();
      DatumWriter<IndexedRecord> datumWriter =
          new GenericDatumWriter<IndexedRecord>(iterSchemas.next());
      EncoderFactory fact = EncoderFactory.get();
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      BinaryEncoder enc = fact.binaryEncoder(stream, null);
      try {
        datumWriter.write(iterRecords.next(), enc);
        enc.flush();
        stream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      iterOutRecords.next().set(new Text(stream.toByteArray()));
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
