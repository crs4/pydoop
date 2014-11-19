/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2014 CRS4.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * END_COPYRIGHT
 */

/*
 * Reads an AvroParquet file and returns raw, binary encoded,
 * serialized avro objects.
 */

package it.crs4.pydoop;

import java.util.Map;
import java.io.IOException;
import java.lang.InterruptedException;
import java.io.ByteArrayOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.generic.GenericData.Record;

import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.util.ContextUtil;


import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import parquet.hadoop.api.ReadSupport;


/*
  Reads an AvroParquet file and returns raw, binary encoded, serialized avro objects.
 */

class PydoopAvroRecordMaterializer extends RecordMaterializer<Text> {
    private RecordMaterializer<IndexedRecord> materializer;
    public  PydoopAvroRecordMaterializer(RecordMaterializer<IndexedRecord> materializer) {
        this.materializer = materializer;
    }
    @Override 
    public Text getCurrentRecord() {
        // FIXME reuse all that can be reused....
        IndexedRecord record = materializer.getCurrentRecord();
        Schema schema = record.getSchema();
        DatumWriter<GenericRecord> datumWriter = 
            new GenericDatumWriter<GenericRecord>(schema);
        EncoderFactory fact = EncoderFactory.get();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        BinaryEncoder enc = fact.binaryEncoder(stream, null);
        try {
            datumWriter.write((GenericData.Record) record, enc);
            enc.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Text v = new Text(stream.toByteArray());
        System.err.println("v.getLength():" + v.getLength());
        return new Text(stream.toByteArray());
    }
    
    @Override
    public GroupConverter getRootConverter() {
        return materializer.getRootConverter();
    }
}

public class PydoopAvroReadSupport extends ReadSupport<Text> {
    private AvroReadSupport<IndexedRecord> read_support;
    
    public PydoopAvroReadSupport() {
        read_support = new AvroReadSupport<IndexedRecord>();
    }
    public static void setRequestedProjection(Configuration configuration, 
                                              Schema requestedProjection) {
        AvroReadSupport.setRequestedProjection(configuration, requestedProjection);
    }

    public static void setAvroReadSchema(Configuration configuration, 
                                         Schema avroReadSchema) {
        AvroReadSupport.setAvroReadSchema(configuration, avroReadSchema);
    }

    @Override
    public ReadContext init(Configuration configuration, 
                            Map<String, String> keyValueMetaData, 
                            MessageType fileSchema) {
        return read_support.init(configuration, keyValueMetaData, fileSchema);
    }
    
    @Override
    public RecordMaterializer<Text> prepareForRead(Configuration configuration, 
                                                   Map<String, String> keyValueMetaData, 
                                                   MessageType fileSchema, 
                                                   ReadContext readContext) {
        RecordMaterializer<IndexedRecord> mat = 
            read_support.prepareForRead(configuration, keyValueMetaData,
                                        fileSchema, readContext);
        return new PydoopAvroRecordMaterializer(mat);
    }
}

