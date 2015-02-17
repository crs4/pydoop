package it.crs4.pydoop.mapreduce.pipes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.Text;

import org.apache.avro.Schema;


public class PydoopAvroOutputValueBridge
    extends OutputFormat<Text, Text> {

  // FIXME: add support for avro keys
  // public static final String AVRO_KEY_SCHEMA =
  //   "pydoop.mapreduce.avro.key.output.schema";
  public static final String AVRO_VALUE_SCHEMA =
    "pydoop.mapreduce.avro.value.output.schema";

  private OutputFormat actualFormat;

  private OutputFormat getActualFormat(Configuration conf) {
    if (actualFormat == null) {
      actualFormat = ReflectionUtils.newInstance(
          conf.getClass(
              Submitter.OUTPUT_FORMAT,
              TextOutputFormat.class,  // default (FIXME: should never happen)
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

  @Override
  public RecordWriter<Text, Text>
      getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Schema schema = Schema.parse(conf.get(AVRO_VALUE_SCHEMA));
    return new PydoopAvroBridgeValueWriter(
        getActualFormat(conf).getRecordWriter(context), schema);
  }

}
