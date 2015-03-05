package it.crs4.pydoop.mapreduce.pipes;

import java.util.Properties;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.io.Text;

import org.apache.avro.Schema;


public class PydoopAvroOutputKeyValueBridge
    extends PydoopAvroOutputBridgeBase {

  public PydoopAvroOutputKeyValueBridge() {
    defaultActualFormat = PydoopAvroKeyValueOutputFormat.class;
  }

  @Override
  public RecordWriter<Text, Text>
      getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Properties props = Submitter.getPydoopProperties();
    Configuration conf = context.getConfiguration();
    Schema keySchema = Schema.parse(conf.get(
        props.getProperty("AVRO_KEY_OUTPUT_SCHEMA")));
    Schema valueSchema = Schema.parse(conf.get(
        props.getProperty("AVRO_VALUE_OUTPUT_SCHEMA")));
    return new PydoopAvroBridgeKeyValueWriter(
        getActualFormat(conf).getRecordWriter(context), keySchema, valueSchema
    );
  }
}
