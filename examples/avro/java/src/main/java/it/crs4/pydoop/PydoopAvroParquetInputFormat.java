/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2015 CRS4.
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
 *
 * Based on AvroParquetInputFormat.
 */

package it.crs4.pydoop;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;

import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.util.ContextUtil;

import org.apache.avro.Schema;


/**
 * A specialized {@link parquet.avro.AvroParquetInputFormat} that can
 * be used with Pydoop.
 */
public class PydoopAvroParquetInputFormat extends ParquetInputFormat<Text> {

  public PydoopAvroParquetInputFormat() {
    super(PydoopAvroReadSupport.class);
  }

  // The following methods are copied verbatim from AvroParquetInputFormat

  public static void setRequestedProjection(
      Job job, Schema requestedProjection) {
    AvroReadSupport.setRequestedProjection(
        ContextUtil.getConfiguration(job), requestedProjection);
  }

  public static void setAvroReadSchema(Job job, Schema avroReadSchema) {
    AvroReadSupport.setAvroReadSchema(
        ContextUtil.getConfiguration(job), avroReadSchema);
  }
}
