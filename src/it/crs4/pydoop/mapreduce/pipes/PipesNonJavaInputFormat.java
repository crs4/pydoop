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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; // FIXME not used
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Dummy input format used when non-Java a {@link RecordReader} is used by
 * the Pipes' application.
 *
 *
 * Sets up the Map-Reduce job to get the {@link PipesDummyRecordReader},
 * and provides a getSplits that will recover InputSplits either:
 * by invoking the getSplits method of 'actual' InputFormat specified by the
 * user in <i>mapreduce.pipes.inputformat</i>;
 * or reading the contents of the hdfs file specificed by the uri
 * <i>mapreduce.pipes.external-splits.path</i> if
 * <i>mapreduce.pipes.external-splits.enabled</i> has been set to
 * <i>true</i>.
 * 
 * The file specified by <i>mapreduce.pipes.external_splits.path</i> should have
 * the structure 
    <WritableInt n><OpaqueObject 1><OpaqueObject 2>..<OpaqueObject n>

 */
class PipesNonJavaInputFormat extends InputFormat<FloatWritable, NullWritable> {
    public static final String EXTERNAL_SPLITS_ENABLED =
        "mapreduce.pipes.external-splits.enabled";
    public static final String EXTERNAL_SPLITS_URI =
        "mapreduce.pipes.external-splits.uri";

    public List<InputSplit> getSplits(JobContext context
                                      ) throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
        boolean splits_enabled = conf.getBoolean(EXTERNAL_SPLITS_ENABLED, false);
        if (splits_enabled) {
            String splits_uri = conf.get(EXTERNAL_SPLITS_URI);
            return getSplitsFromPath(conf, splits_uri);
        } else {
            return ReflectionUtils.newInstance(
                                conf.getClass(Submitter.INPUT_FORMAT, 
                                TextInputFormat.class, 
                                InputFormat.class), conf).getSplits(context);
        }
    }

    private List<InputSplit> getSplitsFromPath(Configuration conf,
                                               String splits_uri
                                               ) throws IOException,
                                                        InterruptedException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(splits_uri);
        if (!fs.exists(path)) {
            throw new IOException(splits_uri + " does not exists");
        }
        FSDataInputStream in = fs.open(path);
        IntWritable n_records = new IntWritable();
        n_records.readFields(in);

        List<InputSplit> splits = new ArrayList<InputSplit>();
        for(int i = 0; i < n_records.get(); i++) {
            OpaqueSplit o = new OpaqueSplit();
            o.readFields(in);
            splits.add(o);
        }
        in.close();
        fs.close();
        return splits;
    }
        

    @Override
    public DummyRecordReader
        createRecordReader(InputSplit split,
                           TaskAttemptContext context)  throws IOException {
        return new PipesDummyRecordReader(split, context);
    }

    /**
     * A dummy {@link org.apache.hadoop.mapreduce.RecordReader} to help track the
     * progress of Hadoop Pipes' applications when they are using a non-Java
     * <code>RecordReader</code>.
     *
     * The <code>PipesDummyRecordReader</code> is informed of the 'progress' of
     * the task by the {@link OutputHandler#progress(float)} which calls the
     * {@link #next(FloatWritable, NullWritable)} with the progress as the
     * <code>key</code>.
     */
    static class PipesDummyRecordReader extends DummyRecordReader {
        float progress = 0.0f;

        public PipesDummyRecordReader() { }
        
        public PipesDummyRecordReader(InputSplit split, TaskAttemptContext context) 
            throws IOException {
            initialize(split, context);
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) 
            throws IOException {
        }
        
        public synchronized void close() throws IOException {}

        @Override
        public float getProgress()  throws IOException, InterruptedException {
            return progress;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return true;
        }

        @Override
        public FloatWritable getCurrentKey() throws IOException, InterruptedException {
            return new FloatWritable(progress);
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
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
