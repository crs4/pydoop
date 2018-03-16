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

package it.crs4.pydoop.examples.pterasort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * An input split consisting of a range of numbers.
 * This mimics the original RangeInputSplit, but we have to extend
 * FileSplit to make pipes happy.
 */
public class RangeInputSplit extends FileSplit {

  long firstRow;
  long rowCount;

  public RangeInputSplit() {}

  public RangeInputSplit(long offset, long length) {
    firstRow = offset;
    rowCount = length;
  }

  @Override
  public Path getPath() {
    return new Path("file:///dev/null");  // not a real FileSplit
  }

  @Override
  public long getStart() {
    return 0;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
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
