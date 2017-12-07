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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
// import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



/**
 * An input split consisting of a range on numbers.
 */
//public class RangeInputSplit extends InputSplit implements Writable {
// FIXME: InputSplit is not Writable, see it.crs4.mapreduce.pipes.PipesMapper
// FIXME: still, it is ugly.
public class RangeInputSplit extends FileSplit {
    long firstRow;
    long rowCount;

    public RangeInputSplit() { }

    public RangeInputSplit(long offset, long length) {
        firstRow = offset;
        rowCount = length;
    }

    /** The file containing this split's data. */
    @Override
    // FIXME: HACK!
    public Path getPath() { return new Path("file:///dev/null"); } 
  
    /** The position of the first byte in the 'file' to process. */
    @Override    
    public long getStart() { return 0; }
  
    /** The number of bytes in the 'file' to process. */
    @Override
    public long getLength() /* throws IOException */ {
        return 0;
    }

    @Override
    public String[] getLocations() /* throws IOException */ {
        return new String[]{};
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstRow = WritableUtils.readVLong(in);
        rowCount = WritableUtils.readVLong(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, firstRow);
        WritableUtils.writeVLong(out, rowCount);
    }
}
