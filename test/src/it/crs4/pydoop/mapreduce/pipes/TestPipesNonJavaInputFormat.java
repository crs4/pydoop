/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.crs4.pydoop.mapreduce.pipes;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestPipesNonJavaInputFormat {
  private static File workSpace = new File("target",
      TestPipesNonJavaInputFormat.class.getName() + "-workSpace");

  /**
   *  test PipesNonJavaInputFormat
    */

  @Test
  public void testFormat() throws IOException, InterruptedException {
      JobID jobId = new JobID("201408272347", 0);
      TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
      TaskAttemptID taskAttemptid = new TaskAttemptID(taskId, 0);

      Job job = new Job(new Configuration());
      job.setJobID(jobId);
      Configuration conf = job.getConfiguration();

      TaskAttemptContextImpl tcontext = 
          new TaskAttemptContextImpl(conf, taskAttemptid);

      PipesNonJavaInputFormat input_format = new PipesNonJavaInputFormat();

      DummyRecordReader reader =  
          (DummyRecordReader) input_format.createRecordReader(new FileSplit(), 
                                                              tcontext);
      assertEquals(0.0f, reader.getProgress(), 0.001);

      // input and output files
      File input1 = new File(workSpace + File.separator + "input1");
      if (!input1.getParentFile().exists()) {
          Assert.assertTrue(input1.getParentFile().mkdirs());
      }

      if (!input1.exists()) {
          Assert.assertTrue(input1.createNewFile());
      }

      File input2 = new File(workSpace + File.separator + "input2");
      if (!input2.exists()) {
          Assert.assertTrue(input2.createNewFile());
      }

      // THIS fill fail without hdfs support.
      // // set data for splits
      // conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR,
      //          StringUtils.escapeString(input1.getAbsolutePath()) + ","
      //          + StringUtils.escapeString(input2.getAbsolutePath()));
      // List<InputSplit> splits = input_format.getSplits(job);
      // assertTrue(splits.size() >= 2);

      PipesNonJavaInputFormat.PipesDummyRecordReader dummyRecordReader = 
          new PipesNonJavaInputFormat.PipesDummyRecordReader(new FileSplit(), tcontext);
      // empty dummyRecordReader
      assertEquals(0.0, dummyRecordReader.getProgress(), 0.001);
      // test method next
      assertTrue(dummyRecordReader.next(new FloatWritable(2.0f), NullWritable.get()));
      assertEquals(2.0, dummyRecordReader.getProgress(), 0.001);
      dummyRecordReader.close();
  }
}
