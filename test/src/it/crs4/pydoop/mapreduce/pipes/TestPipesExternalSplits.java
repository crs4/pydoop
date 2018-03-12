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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test PipesNonJavaInputFormat external splits support.
 */
public class TestPipesExternalSplits {

  @Test
  public void testExternalSplitsSupport()
      throws IOException, InterruptedException {
    JobID jobId = new JobID("201408272347", 0);
    TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
    TaskAttemptID taskAttemptid = new TaskAttemptID(taskId, 0);

    Job job = new Job(new Configuration());
    job.setJobID(jobId);
    Configuration conf = job.getConfiguration();
    // FIXME this is hardwired. Use tmpfile or something like that.
    final String uri = new String("a_binary_file.bin");
    conf.setBoolean(PipesNonJavaInputFormat.EXTERNAL_SPLITS_ENABLED, true);
    conf.set(PipesNonJavaInputFormat.EXTERNAL_SPLITS_URI, uri);

    TaskAttemptContextImpl tcontext =
        new TaskAttemptContextImpl(conf, taskAttemptid);

    PipesNonJavaInputFormat input_format = new PipesNonJavaInputFormat();
    int n_splits = 10;
    List<InputSplit> written = write_input_splits(n_splits, conf, uri);
    List<InputSplit> read = input_format.getSplits(tcontext);

    assertEquals(written.size(), n_splits);
    assertEquals(written.size(), read.size());
    for(int i = 0; i < read.size(); i++) {
      OpaqueSplit itwas = (OpaqueSplit) written.get(i);
      OpaqueSplit itis = (OpaqueSplit) read.get(i);
      assertEquals(itwas.getCode(), itis.getCode());
      assertEquals(itwas.getPayload(), itis.getPayload());
    }
  }

  private List<InputSplit> write_input_splits(
      int n_splits, Configuration conf, String uri)
      throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(uri);
    FSDataOutputStream out = fs.create(path);
    IntWritable n_records = new IntWritable(n_splits);
    n_records.write(out);
    List<InputSplit> splits = new ArrayList<InputSplit>();

    for(int i = 0; i < n_records.get(); i++) {
      String code = "code-" + i;
      String payload = "payload-" + i;
      OpaqueSplit osplit = new OpaqueSplit(code.getBytes(), payload.getBytes());
      osplit.write(out);
      splits.add(osplit);
    }

    out.close();
    fs.close();
    return splits;
  }
}
