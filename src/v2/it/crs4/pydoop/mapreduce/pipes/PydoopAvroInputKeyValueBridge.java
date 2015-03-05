package it.crs4.pydoop.mapreduce.pipes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class PydoopAvroInputKeyValueBridge
    extends PydoopAvroInputBridgeBase<Text, Text> {

  public PydoopAvroInputKeyValueBridge() {
    defaultActualFormat = PydoopAvroKeyValueInputFormat.class;
  }

  @Override
  public RecordReader<Text, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return new PydoopAvroBridgeKeyValueReader(
        getActualFormat(conf).createRecordReader(split, context));
  }
}
