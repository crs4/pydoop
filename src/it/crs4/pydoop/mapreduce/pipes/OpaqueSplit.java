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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package it.crs4.pydoop.mapreduce.pipes;

import org.apache.hadoop.mapred.SplitLocationInfo;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * An opaque piece of information to be handled on the client side.
 */
class OpaqueSplit extends InputSplit implements Writable {

  private BytesWritable payload;

  public OpaqueSplit() {
    payload = new BytesWritable();
  }

  public OpaqueSplit(byte[] payload) {
    this.payload = new BytesWritable(payload);
  }

  public BytesWritable getPayload() {
    return payload;
  }

  @Override
  public long getLength() {
    return payload.getLength();
  }

  @Override
  public String toString() {
    return payload.toString();
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[]{};
  }

  @Override
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return new SplitLocationInfo[]{};
  }

  // Writable methods

  @Override
  public void write(DataOutput out) throws IOException {
    payload.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    payload.readFields(in);
  }

}
