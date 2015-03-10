package it.crs4.pydoop.mapreduce.pipes;

import java.util.List;
import java.util.Arrays;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class PydoopAvroBridgeValueWriter extends PydoopAvroBridgeWriterBase {

  private final Schema schema;

  public PydoopAvroBridgeValueWriter(
      RecordWriter<NullWritable, ? super GenericRecord> actualWriter,
      Schema schema) {
    this.actualWriter = actualWriter;
    this.schema = schema;
  }

  public void write(Text ignore, Text value)
      throws IOException, InterruptedException {
    List<GenericRecord> outRecords = super.getOutRecords(
        Arrays.asList(value), Arrays.asList(schema));
    assert outRecords.size() == 1;
    // actualWriter.write(NullWritable.get(), outRecords.get(0));
    actualWriter.write(null, outRecords.get(0));
  }

}
