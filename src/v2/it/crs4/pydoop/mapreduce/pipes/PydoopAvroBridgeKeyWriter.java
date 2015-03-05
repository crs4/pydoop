package it.crs4.pydoop.mapreduce.pipes;

import java.util.List;
import java.util.Arrays;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class PydoopAvroBridgeKeyWriter extends PydoopAvroBridgeWriterBase {

  private final Schema schema;

  public PydoopAvroBridgeKeyWriter(
      RecordWriter<? super GenericRecord, NullWritable> actualWriter,
      Schema schema) {
    this.actualWriter = actualWriter;
    this.schema = schema;
  }

  public void write(Text key, Text ignore)
      throws IOException, InterruptedException {
    List<GenericRecord> outRecords = super.getOutRecords(
        Arrays.asList(key), Arrays.asList(schema));
    assert outRecords.size() == 1;
    actualWriter.write(outRecords.get(0), NullWritable.get());
    // actualWriter.write(null, outRecords.get(0));
  }

}
