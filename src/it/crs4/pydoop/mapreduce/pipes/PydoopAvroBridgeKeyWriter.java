package it.crs4.pydoop.mapreduce.pipes;

import java.util.List;
import java.util.Arrays;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.avro.generic.GenericRecord;


public class PydoopAvroBridgeKeyWriter extends PydoopAvroBridgeWriterBase {

  public PydoopAvroBridgeKeyWriter(
      RecordWriter<? super GenericRecord, NullWritable> actualWriter,
      TaskAttemptContext context) {
    super(context, Submitter.AvroIO.K);
    this.actualWriter = actualWriter;
  }

  public void write(Text key, Text ignore)
      throws IOException, InterruptedException {
    List<GenericRecord> outRecords = super.getOutRecords(Arrays.asList(key));
    super.write(outRecords);
  }

}
