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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class PydoopAvroInputKeyValueBridge
    extends PydoopAvroInputBridgeBase<Text, Text> {

  public PydoopAvroInputKeyValueBridge() {
    defaultActualFormat = PydoopAvroKeyValueInputFormat.class;
  }

  @Override
  public RecordReader<Text, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return new PydoopAvroBridgeKeyValueReader(
        getActualFormat(conf).createRecordReader(split, context));
  }
}
