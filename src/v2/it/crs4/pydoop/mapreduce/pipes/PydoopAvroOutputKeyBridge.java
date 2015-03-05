package it.crs4.pydoop.mapreduce.pipes;

import java.util.Properties;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.io.Text;

import org.apache.avro.Schema;


public class PydoopAvroOutputKeyBridge extends PydoopAvroOutputBridgeBase {

  public PydoopAvroOutputKeyBridge() {
    defaultActualFormat = PydoopAvroKeyOutputFormat.class;
  }

  @Override
  public RecordWriter<Text, Text>
      getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Properties props = Submitter.getPydoopProperties();
    Configuration conf = context.getConfiguration();
    Schema schema = Schema.parse(conf.get(
        props.getProperty("AVRO_KEY_OUTPUT_SCHEMA")));
    return new PydoopAvroBridgeKeyWriter(
        getActualFormat(conf).getRecordWriter(context), schema);
  }
}
