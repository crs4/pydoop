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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.mapred.FsInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class PydoopAvroRecordReaderBase<K, V>
    extends RecordReader<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(
      PydoopAvroRecordReaderBase.class);

  private final Schema mReaderSchema;
  private GenericRecord mCurrentRecord;
  private DataFileReader<GenericRecord> mAvroFileReader;
  private long mStartPosition;
  private long mEndPosition;

  protected PydoopAvroRecordReaderBase(Schema readerSchema) {
    mReaderSchema = readerSchema;
    mCurrentRecord = null;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    if (!(inputSplit instanceof FileSplit)) {
      throw new IllegalArgumentException("Only compatible with FileSplits.");
    }
    FileSplit fileSplit = (FileSplit) inputSplit;
    SeekableInput seekableFileInput = createSeekableInput(
        context.getConfiguration(), fileSplit.getPath());
    mAvroFileReader = new DataFileReader<GenericRecord>(seekableFileInput,
        new GenericDatumReader<GenericRecord>(mReaderSchema));
    // We will read the first block that begins after the input split
    // start; we will read up to but not including the first block
    // that begins after the input split end.
    mAvroFileReader.sync(fileSplit.getStart());
    mStartPosition = mAvroFileReader.previousSync();
    mEndPosition = fileSplit.getStart() + fileSplit.getLength();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    assert null != mAvroFileReader;
    if (mAvroFileReader.hasNext() && !mAvroFileReader.pastSync(mEndPosition)) {
      mCurrentRecord = mAvroFileReader.next(mCurrentRecord);
      return true;
    }
    return false;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    assert null != mAvroFileReader;
    if (mEndPosition == mStartPosition) {
      return 0.0f;
    }
    long bytesRead = mAvroFileReader.previousSync() - mStartPosition;
    long bytesTotal = mEndPosition - mStartPosition;
    LOG.debug(
        "Progress: bytesRead=" + bytesRead + ", bytesTotal=" + bytesTotal);
    return Math.min(1.0f, (float) bytesRead / (float) bytesTotal);
  }

  @Override
  public void close() throws IOException {
    if (null != mAvroFileReader) {
      try {
        mAvroFileReader.close();
      } finally {
        mAvroFileReader = null;
      }
    }
  }

  protected GenericRecord getCurrentRecord() {
    return mCurrentRecord;
  }

  protected SeekableInput createSeekableInput(Configuration conf, Path path)
      throws IOException {
    return new FsInput(path, conf);
  }
}
