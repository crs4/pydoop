package it.crs4.pydoop.mapreduce.pipes;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;


public abstract class PydoopAvroInputBridgeBase<K, V>
    extends InputFormat<K, V> {

  protected InputFormat actualFormat;
  protected Class<? extends InputFormat> defaultActualFormat;

  protected InputFormat getActualFormat(Configuration conf) {
    if (actualFormat == null) {
      actualFormat = ReflectionUtils.newInstance(
          conf.getClass(
              Submitter.INPUT_FORMAT,
              defaultActualFormat,
              InputFormat.class), conf);
        }
    return actualFormat;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return getActualFormat(conf).getSplits(context);
  }

}
