package it.crs4.pydoop.mapreduce.pipes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.Text;


public abstract class PydoopAvroOutputBridgeBase
    extends OutputFormat<Text, Text> {

  protected OutputFormat actualFormat;
  protected Class<? extends OutputFormat> defaultActualFormat;

  protected OutputFormat getActualFormat(Configuration conf) {
    if (actualFormat == null) {
      actualFormat = ReflectionUtils.newInstance(
          conf.getClass(
              Submitter.OUTPUT_FORMAT,
              defaultActualFormat,
              OutputFormat.class), conf);
        }
    return actualFormat;
  }

  @Override
  public void checkOutputSpecs(JobContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    getActualFormat(conf).checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return getActualFormat(conf).getOutputCommitter(context);
  }
}
