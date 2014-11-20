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
import java.util.List;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;


public class DummyInputFormat extends InputFormat<FloatWritable, NullWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext context) 
        throws IOException, InterruptedException {
        InputSplit splits[] =  {new FileSplit(new Path("file://foobar"), 0, 100, null)};
        return Arrays.asList(splits);
    }

    @Override
    public DummyRecordReader
        createRecordReader(InputSplit split, TaskAttemptContext context) {
        ReaderPipesMapper reader = new ReaderPipesMapper();
        reader.initialize(split, context);
        return reader;
    }

    private class ReaderPipesMapper extends DummyRecordReader {
        private int index = 0;
        private int max_index = 10;
        private FloatWritable key = new FloatWritable(0.0f);
        private NullWritable value = NullWritable.get();

        @Override
            public void initialize(InputSplit split, TaskAttemptContext context) {
        }

        public boolean next(FloatWritable key, NullWritable value)
            throws IOException {
            this.key = key;
            System.err.println("progress:" + this.key);
            return true;
        }

        @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
            key.set(index++);
            return index <= max_index;
        }

        @Override
            public FloatWritable getCurrentKey() 
            throws IOException, InterruptedException {
            return key;
        }

        @Override
            public NullWritable getCurrentValue() 
            throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException {
            return key.get();
        }

        @Override
            public void close() {}
    }

}




