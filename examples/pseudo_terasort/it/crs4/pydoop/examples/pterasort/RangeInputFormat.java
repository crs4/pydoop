// BEGIN_COPYRIGHT
// 
// Copyright 2009-2016 CRS4.
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

package it.crs4.pydoop.examples.pterasort;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * An input format that assigns ranges of longs to each mapper.
 */
public class RangeInputFormat 
    extends InputFormat<LongWritable, NullWritable> {
    private static final Log LOG = LogFactory.getLog(RangeInputFormat.class);
    
    public RecordReader<LongWritable, NullWritable> 
        createRecordReader(InputSplit split, TaskAttemptContext context) 
        throws IOException {
        return new RangeRecordReader();
    }

    /**
     * Create the desired number of splits, dividing the number of rows
     * between the mappers.
     */
    public List<InputSplit> getSplits(JobContext job) {
        long totalRows = getNumberOfRows(job);
        int numSplits = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
        LOG.info("Generating " + totalRows + " using " + numSplits);
        List<InputSplit> splits = new ArrayList<InputSplit>();
        long currentRow = 0;
        for(int split = 0; split < numSplits; ++split) {
            long goal = 
                (long) Math.ceil(totalRows * (double)(split + 1) / numSplits);
            splits.add(new RangeInputSplit(currentRow, goal - currentRow));
            currentRow = goal;
        }
        return splits;
    }

    static long getNumberOfRows(JobContext job) {
        return job.getConfiguration().getLong("mapreduce.pterasort.num-rows", 0L);
    }
  
    static void setNumberOfRows(Job job, long numRows) {
        job.getConfiguration().setLong("mapreduce.pterasort.num-rows", numRows);
    }
}


