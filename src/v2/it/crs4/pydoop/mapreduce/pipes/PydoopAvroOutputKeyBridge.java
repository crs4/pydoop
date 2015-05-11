package it.crs4.pydoop.mapreduce.pipes;

import java.io.IOException;

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
    return new PydoopAvroBridgeKeyWriter(
        getActualFormat(context.getConfiguration()).getRecordWriter(context),
        context
    );
  }
}
