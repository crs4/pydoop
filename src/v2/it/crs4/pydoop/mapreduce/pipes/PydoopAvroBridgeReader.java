package it.crs4.pydoop.mapreduce.pipes;

import java.io.IOException;
import java.io.ByteArrayOutputStream;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.BinaryEncoder;


public class PydoopAvroBridgeReader extends RecordReader<LongWritable, Text> {

  // FIXME: add support for avro keys
  public static final String AVRO_VALUE_SCHEMA =
    "pydoop.mapreduce.pipes.avro.value.schema";

  private final RecordReader<?, ? extends IndexedRecord> actualReader;
  private LongWritable key;
  private Text value;
  private IndexedRecord bufferedRecord;
  private Schema schema;

  public PydoopAvroBridgeReader(
      RecordReader<?, ? extends IndexedRecord> actualReader) {
    this.actualReader = actualReader;
  }

  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    actualReader.initialize(split, context);
    // get a record so we can set the schema property
    if (actualReader.nextKeyValue()) {
      bufferedRecord = actualReader.getCurrentValue();
      schema = bufferedRecord.getSchema();
      context.getConfiguration().set(AVRO_VALUE_SCHEMA, schema.toString());
      key = new LongWritable();
      value = new Text();
      key.set(0);  // FIXME
    }
  }

  @Override
  public LongWritable getCurrentKey()
      throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue()
      throws IOException, InterruptedException {
    return value;
  }

  public synchronized boolean nextKeyValue()
      throws IOException, InterruptedException {
    IndexedRecord record;
    if (bufferedRecord == null) {
      if (!actualReader.nextKeyValue()) {
        return false;
      }
      else {
        record = actualReader.getCurrentValue();
      }
    }
    else {
      record = bufferedRecord;
      bufferedRecord = null;
    }
    DatumWriter<GenericRecord> datumWriter =
        new GenericDatumWriter<GenericRecord>(schema);
    EncoderFactory fact = EncoderFactory.get();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    BinaryEncoder enc = fact.binaryEncoder(stream, null);
    try {
      datumWriter.write((GenericData.Record) record, enc);
      enc.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    value.set(new Text(stream.toByteArray()));
    return true;
  }

  public float getProgress() throws IOException,  InterruptedException {
    return actualReader.getProgress();
  }

  public synchronized void close() throws IOException {
    actualReader.close();
  }

}
