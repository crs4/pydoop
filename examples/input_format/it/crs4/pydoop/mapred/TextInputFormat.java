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

package it.crs4.pydoop.mapred;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TextInputFormat extends FileInputFormat<LongWritable, Text>
    implements JobConfigurable {
    
    private Boolean will_split;

    public void configure(JobConf conf) {
        will_split = conf.getBoolean("pydoop.input.issplitable", true);
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        return will_split;
    }
    
    public RecordReader<LongWritable, Text> getRecordReader(
                                          InputSplit genericSplit, JobConf job,
                                          Reporter reporter)
	throws IOException {
        reporter.setStatus(genericSplit.toString());
        return new LineRecordReader(job, (FileSplit) genericSplit);
    }
}
