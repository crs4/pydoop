package it.crs4.pydoop.mapreduce.pipes;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.util.ReflectionUtils;


public class PydoopAvroInputKeyBridge
    extends InputFormat<Text, NullWritable> {

  private InputFormat actualFormat;

  // FIXME: move this to a new PydoopAvroInputBridgeBase class
  private InputFormat getActualFormat(Configuration conf) {
    if (actualFormat == null) {
      actualFormat = ReflectionUtils.newInstance(
          conf.getClass(
              Submitter.INPUT_FORMAT,
              TextInputFormat.class,  // default (FIXME: should never happen)
              InputFormat.class), conf);
        }
    return actualFormat;
  }

  @Override
  public RecordReader<Text, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return new PydoopAvroBridgeKeyReader(
        getActualFormat(conf).createRecordReader(split, context));
  }


  // FIXME: move this to a new PydoopAvroInputBridgeBase class
  @Override
  public List<InputSplit> getSplits(JobContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return getActualFormat(conf).getSplits(context);
  }

}