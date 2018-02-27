// BEGIN_COPYRIGHT
//
// Copyright 2009-2018 CRS4.
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

import java.util.List;
import java.util.Arrays;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.avro.generic.GenericRecord;


public class PydoopAvroBridgeKeyWriter extends PydoopAvroBridgeWriterBase {

  public PydoopAvroBridgeKeyWriter(
      RecordWriter<? super GenericRecord, NullWritable> actualWriter,
      TaskAttemptContext context) {
    super(context, Submitter.AvroIO.K);
    this.actualWriter = actualWriter;
  }

  public void write(Text key, Text ignore)
      throws IOException, InterruptedException {
    List<GenericRecord> outRecords = super.getOutRecords(Arrays.asList(key));
    super.write(outRecords);
  }

}
