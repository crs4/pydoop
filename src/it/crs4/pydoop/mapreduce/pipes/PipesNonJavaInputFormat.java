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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Dummy input format used when non-Java a {@link RecordReader} is used by
 * the Pipes' application.
 *
 * Sets up the Map-Reduce job to get the {@link PipesDummyRecordReader} and
 * the input splits. If <i>pydoop.mapreduce.pipes.externalsplits.uri</i> is
 * defined, input splits are read from the specified HDFS URI as a binary
 * sequence in the following format: <N><OBJ_1><OBJ_2>...<OBJ_N>, i.e., a
 * WritableInt N followed by N opaque objects. If it's not defined, input
 * splits are retrieved by invoking the getSplits method of the 'actual'
 * InputFormat specified by the user in <i>mapreduce.pipes.inputformat</i>.
 */
class PipesNonJavaInputFormat
    extends InputFormat<FloatWritable, NullWritable> {

  public List<InputSplit> getSplits(JobContext context)
      throws IOException, InterruptedException {
    Properties props = Submitter.getPydoopProperties();
    Configuration conf = context.getConfiguration();
    String uri = conf.get(props.getProperty("PIPES_EXTERNALSPLITS_URI"));
    if (uri != null) {
      return getOpaqueSplits(conf, uri);
    } else {
      return ReflectionUtils.newInstance(
          conf.getClass(Submitter.INPUT_FORMAT,
                        TextInputFormat.class,
                        InputFormat.class), conf).getSplits(context);
    }
  }

  private List<InputSplit> getOpaqueSplits(Configuration conf, String uri)
      throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(uri);
    if (!fs.exists(path)) {
      throw new IOException(uri + " does not exists");
    }
    List<InputSplit> splits = new ArrayList<InputSplit>();
    FSDataInputStream in = fs.open(path);
    try {
      IntWritable numRecords = new IntWritable();
      numRecords.readFields(in);
      for(int i = 0; i < numRecords.get(); i++) {
        OpaqueSplit o = new OpaqueSplit();
        o.readFields(in);
        splits.add(o);
      }
    } finally {
      in.close();
    }
    return splits;
  }

  @Override
  public DummyRecordReader
    createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new PipesDummyRecordReader(split, context);
  }

  /**
   * A dummy {@link org.apache.hadoop.mapreduce.RecordReader} to help track the
   * progress of Hadoop Pipes applications when they are using a non-Java
   * <code>RecordReader</code>.
   *
   * The <code>PipesDummyRecordReader</code> is informed of the 'progress' of
   * the task by the {@link OutputHandler#progress(float)} which calls the
   * {@link #next(FloatWritable, NullWritable)} with the progress as the
   * <code>key</code>.
   */
  static class PipesDummyRecordReader extends DummyRecordReader {

    float progress = 0.0f;

    public PipesDummyRecordReader() {}

    public PipesDummyRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException {
      initialize(split, context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException {}

    public synchronized void close() throws IOException {}

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return progress;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return true;
    }

    @Override
    public FloatWritable getCurrentKey()
        throws IOException, InterruptedException {
      return new FloatWritable(progress);
    }

    @Override
    public NullWritable getCurrentValue()
        throws IOException, InterruptedException {
      return null;
    }

    @Override
    public synchronized boolean next(FloatWritable key, NullWritable value)
        throws IOException  {
      progress = key.get();
      return true;
    }
  }

}
