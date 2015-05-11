package it.crs4.pydoop.mapreduce.pipes;

import java.util.List;
import java.util.Arrays;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.avro.generic.GenericRecord;


public class PydoopAvroBridgeValueWriter extends PydoopAvroBridgeWriterBase {

  public PydoopAvroBridgeValueWriter(
      RecordWriter<NullWritable, ? super GenericRecord> actualWriter,
      TaskAttemptContext context) {
    super(context, Submitter.AvroIO.V);
    this.actualWriter = actualWriter;
  }

  public void write(Text ignore, Text value)
      throws IOException, InterruptedException {
    List<GenericRecord> outRecords = super.getOutRecords(Arrays.asList(value));
    super.write(outRecords);
  }

}
