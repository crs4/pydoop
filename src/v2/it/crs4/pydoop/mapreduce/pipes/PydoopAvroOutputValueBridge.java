package it.crs4.pydoop.mapreduce.pipes;

import java.util.Properties;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.io.Text;

import org.apache.avro.Schema;


public class PydoopAvroOutputValueBridge extends PydoopAvroOutputBridgeBase {

  public PydoopAvroOutputValueBridge() {
    defaultActualFormat = PydoopAvroValueOutputFormat.class;
  }

  @Override
  public RecordWriter<Text, Text>
      getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Properties props = Submitter.getPydoopProperties();
    Configuration conf = context.getConfiguration();
    Schema schema = Schema.parse(conf.get(
        props.getProperty("AVRO_VALUE_OUTPUT_SCHEMA")));
    return new PydoopAvroBridgeValueWriter(
        getActualFormat(conf).getRecordWriter(context), schema);
  }
}
