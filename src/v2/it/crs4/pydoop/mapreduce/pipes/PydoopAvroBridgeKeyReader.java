package it.crs4.pydoop.mapreduce.pipes;

import java.util.Properties;
import java.util.List;
import java.util.Arrays;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import org.apache.avro.generic.IndexedRecord;


public class PydoopAvroBridgeKeyReader
    extends PydoopAvroBridgeReaderBase<Text, NullWritable> {

  private Properties props;

  public PydoopAvroBridgeKeyReader(
      RecordReader<? extends IndexedRecord, ?> actualReader) {
    this.actualReader = actualReader;
    props = Submitter.getPydoopProperties();
  }

  protected List<IndexedRecord> getInRecords()
      throws IOException, InterruptedException {
    IndexedRecord key = (IndexedRecord) actualReader.getCurrentKey();
    return Arrays.asList(key);
  }

  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    super.initialize(split, context);
    assert schemas.size() == 1;
    Configuration conf = context.getConfiguration();
    conf.set(props.getProperty("AVRO_INPUT"), Submitter.AvroIO.K.name());
    conf.set(props.getProperty("AVRO_KEY_INPUT_SCHEMA"),
        schemas.get(0).toString());
  }

  @Override
  public Text getCurrentKey()
      throws IOException, InterruptedException {
    assert outRecords.size() == 1;
    return outRecords.get(0);
  }

  @Override
  public NullWritable getCurrentValue()
      throws IOException, InterruptedException {
    return NullWritable.get();
  }
}
