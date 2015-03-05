package it.crs4.pydoop.mapreduce.pipes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class PydoopAvroInputValueBridge
    extends PydoopAvroInputBridgeBase<NullWritable, Text> {

  public PydoopAvroInputValueBridge() {
    defaultActualFormat = PydoopAvroValueInputFormat.class;
  }

  @Override
  public RecordReader<NullWritable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return new PydoopAvroBridgeValueReader(
        getActualFormat(conf).createRecordReader(split, context));
  }
}
