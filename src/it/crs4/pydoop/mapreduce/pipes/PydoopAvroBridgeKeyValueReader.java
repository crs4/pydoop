// BEGIN_COPYRIGHT
//
// Copyright 2009-2022 CRS4.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
//
// END_COPYRIGHT

package it.crs4.pydoop.mapreduce.pipes;

import java.util.Properties;
import java.util.List;
import java.util.Arrays;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import org.apache.avro.generic.IndexedRecord;


public class PydoopAvroBridgeKeyValueReader
    extends PydoopAvroBridgeReaderBase<Text, Text> {

  private Properties props;

  public PydoopAvroBridgeKeyValueReader(
      RecordReader<? extends IndexedRecord,
                   ? extends IndexedRecord> actualReader) {
    this.actualReader = actualReader;
    props = Submitter.getPydoopProperties();
  }

  protected List<IndexedRecord> getInRecords()
      throws IOException, InterruptedException {
    IndexedRecord key = (IndexedRecord) actualReader.getCurrentKey();
    IndexedRecord value = (IndexedRecord) actualReader.getCurrentValue();
    return Arrays.asList(key, value);
  }

  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    super.initialize(split, context);
    assert schemas.size() == 2;
    Configuration conf = context.getConfiguration();
    conf.set(props.getProperty("AVRO_INPUT"), Submitter.AvroIO.KV.name());
    conf.set(props.getProperty("AVRO_KEY_INPUT_SCHEMA"),
        schemas.get(0).toString());
    conf.set(props.getProperty("AVRO_VALUE_INPUT_SCHEMA"),
        schemas.get(1).toString());
  }

  @Override
  public Text getCurrentKey()
      throws IOException, InterruptedException {
    assert outRecords.size() == 2;
    return outRecords.get(0);
  }

  @Override
  public Text getCurrentValue()
      throws IOException, InterruptedException {
    assert outRecords.size() == 2;
    return outRecords.get(1);
  }
}
