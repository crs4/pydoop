/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2019 CRS4.
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
import java.util.List;
import java.util.Properties;

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


/**
 * Use PipesNonJavaInputFormat.getSplits to read opaque splits from inUri,
 * then write them out to outUri.
 */

public class OpaqueRoundtrip {

  public static void main(String[] args)
      throws IOException, InterruptedException {
    final String inUri = args[0];
    final String outUri = args[1];
    JobID jobId = new JobID("201408272347", 0);
    TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
    TaskAttemptID taID = new TaskAttemptID(taskId, 0);
    Job job = Job.getInstance(new Configuration());
    job.setJobID(jobId);
    Properties props = Submitter.getPydoopProperties();
    Configuration conf = job.getConfiguration();
    conf.set(props.getProperty("PIPES_EXTERNALSPLITS_URI"), inUri);
    TaskAttemptContextImpl ctx = new TaskAttemptContextImpl(conf, taID);
    PipesNonJavaInputFormat iformat = new PipesNonJavaInputFormat();
    List<InputSplit> splits = iformat.getSplits(ctx);
    Path path = new Path(outUri);
    FileSystem fs = FileSystem.get(conf);
    IntWritable numRecords = new IntWritable(splits.size());
    FSDataOutputStream out = fs.create(path);
    try {
      numRecords.write(out);
      for(int i = 0; i < numRecords.get(); i++) {
        ((OpaqueSplit)splits.get(i)).write(out);
      }
    } finally {
      out.close();
    }
    fs.close();
  }

}
