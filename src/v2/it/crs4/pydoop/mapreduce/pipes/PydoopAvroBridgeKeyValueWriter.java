package it.crs4.pydoop.mapreduce.pipes;

import java.util.List;
import java.util.Arrays;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.io.Text;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class PydoopAvroBridgeKeyValueWriter
    extends PydoopAvroBridgeWriterBase {

  private final Schema keySchema;
  private final Schema valueSchema;

  public PydoopAvroBridgeKeyValueWriter(
      RecordWriter<? super GenericRecord, ? super GenericRecord> actualWriter,
      Schema keySchema, Schema valueSchema
  ) {
    this.actualWriter = actualWriter;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
  }

  public void write(Text key, Text value)
      throws IOException, InterruptedException {
    List<GenericRecord> outRecords = super.getOutRecords(
        Arrays.asList(key, value), Arrays.asList(keySchema, valueSchema));
    assert outRecords.size() == 2;
    actualWriter.write(outRecords.get(0), outRecords.get(1));
  }
}
