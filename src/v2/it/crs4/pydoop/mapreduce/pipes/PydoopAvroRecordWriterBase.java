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
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public abstract class PydoopAvroRecordWriterBase<K, V>
    extends RecordWriter<K, V> {

  protected final DataFileWriter<GenericRecord> mAvroFileWriter;

  protected PydoopAvroRecordWriterBase(Schema writerSchema,
      CodecFactory compressionCodec, OutputStream outputStream)
      throws IOException {
    mAvroFileWriter = new DataFileWriter<GenericRecord>(
        new GenericDatumWriter<GenericRecord>(writerSchema));
    mAvroFileWriter.setCodec(compressionCodec);
    mAvroFileWriter.create(writerSchema, outputStream);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException {
    mAvroFileWriter.close();
  }
}
