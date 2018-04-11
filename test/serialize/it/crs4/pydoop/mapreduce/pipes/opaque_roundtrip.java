/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2018 CRS4.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * END_COPYRIGHT
 */

package it.crs4.pydoop.mapreduce.pipes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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


public class opaque_roundtrip {

	public static void main(String[] args)
      throws java.io.IOException, InterruptedException {
      final String in_uri = args[0];
      final String out_uri = args[1];

      JobID jobId = new JobID("201408272347", 0);
      TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
      TaskAttemptID taskAttemptid = new TaskAttemptID(taskId, 0);

      Job job = new Job(new Configuration());
      job.setJobID(jobId);
      Configuration conf = job.getConfiguration();
      conf.set(PipesNonJavaInputFormat.EXTERNAL_SPLITS_URI, in_uri);
      TaskAttemptContextImpl tcontext =
          new TaskAttemptContextImpl(conf, taskAttemptid);
      PipesNonJavaInputFormat iformat = new PipesNonJavaInputFormat();
      List<InputSplit> read = iformat.getSplits(tcontext);

      Path path = new Path(out_uri);
      FileSystem fs = FileSystem.get(conf);
      write_input_splits(read, fs, path);
      fs.close();
  }

  private static void write_input_splits(List<InputSplit> splits,
                                         FileSystem fs, Path path)
      throws IOException, InterruptedException {
    IntWritable n_records = new IntWritable(splits.size());
    FSDataOutputStream out = fs.create(path);
    try {
      n_records.write(out);
      for(int i = 0; i < n_records.get(); i++) {
        ((OpaqueSplit)splits.get(i)).write(out);
      }
    } finally {
      out.close();
    }
  }
}
