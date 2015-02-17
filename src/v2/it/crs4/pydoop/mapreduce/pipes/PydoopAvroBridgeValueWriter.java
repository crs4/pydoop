package it.crs4.pydoop.mapreduce.pipes;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Decoder;


public class PydoopAvroBridgeValueWriter
    extends RecordWriter<Text, Text> {

  private final RecordWriter<NullWritable, ? super GenericRecord>
      actualWriter;
  private final Schema schema;

  public PydoopAvroBridgeValueWriter(
      RecordWriter<NullWritable, ? super GenericRecord> actualWriter,
      Schema schema) {
    this.actualWriter = actualWriter;
    this.schema = schema;
  }

  // FIXME: this should be parametrized: Text, Text is the default, but the
  // user can change types by setting MRJobConfig.OUTPUT_KEY{,VALUE}_CLASS
  public void write(Text ignore, Text value)
      throws IOException, InterruptedException {
    // FIXME: add support for keys
    DatumReader<GenericRecord> reader =
        new GenericDatumReader<GenericRecord>(schema);
    DecoderFactory fact = DecoderFactory.get();
    Decoder dec = fact.binaryDecoder(value.copyBytes(), null);
    GenericRecord r = reader.read(null, dec);
    // actualWriter.write(NullWritable.get(), r);
    actualWriter.write(null, r);
  }

  public void close(TaskAttemptContext context)
      throws IOException, InterruptedException {
    actualWriter.close(context);
  }

}
