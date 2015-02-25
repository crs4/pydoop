package it.crs4.pydoop.mapreduce.pipes;

import java.util.Properties;
import java.io.IOException;
import java.io.ByteArrayOutputStream;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.BinaryEncoder;


public class PydoopAvroBridgeValueReader
    extends RecordReader<NullWritable, Text> {

  private final RecordReader<?, ? extends IndexedRecord> actualReader;
  private Text value;
  private IndexedRecord bufferedRecord;
  private Schema schema;
  private Properties props;

  public PydoopAvroBridgeValueReader(
      RecordReader<?, ? extends IndexedRecord> actualReader) {
    this.actualReader = actualReader;
    props = Submitter.getPydoopProperties();
  }

  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    actualReader.initialize(split, context);
    Configuration conf = context.getConfiguration();
    conf.set(props.getProperty("AVRO_INPUT"), Submitter.AvroIO.V.name());
    // get a record so we can set the schema property
    if (actualReader.nextKeyValue()) {
      bufferedRecord = actualReader.getCurrentValue();
      schema = bufferedRecord.getSchema();
      conf.set(props.getProperty("AVRO_VALUE_INPUT_SCHEMA"), schema.toString());
      value = new Text();
    }
  }

  @Override
  public NullWritable getCurrentKey()
      throws IOException, InterruptedException {
    return NullWritable.get();
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
      stream.close();
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
