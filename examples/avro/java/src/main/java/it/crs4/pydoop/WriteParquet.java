/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2018 CRS4.
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
 * A MapReduce application that reads ';'-separated text and writes
 * parquet-avro data (i.e., Parquet files that use the Avro object model).
 *
 * Based on Cloudera Parquet examples.
 */

package it.crs4.pydoop;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.parquet.Log;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.Schema;


public class WriteParquet extends Configured implements Tool {

  private static final Log LOG = Log.getLog(WriteParquet.class);

  // FIXME: not needed, we're calling setSchema below
  private static final String SCHEMA_PATH_KEY = "paexample.schema.path";

  private static Schema getSchema(Configuration conf)
      throws IOException {
    Path schemaPath = new Path(conf.get(SCHEMA_PATH_KEY));
    FileSystem fs = FileSystem.get(conf);
    InputStream in = fs.open(schemaPath);
    Schema schema = new Schema.Parser().parse(in);
    in.close();
    return schema;
  }

  public static class WriteUserMap
      extends Mapper<LongWritable, Text, NullWritable, Record> {

    private Schema schema;

    @Override
    public void setup(Context context)
        throws IOException, InterruptedException {
      schema = getSchema(context.getConfiguration());
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      NullWritable outKey = NullWritable.get();
      Record user = new Record(schema);
      String[] elements = value.toString().split(";");
      user.put("name", elements[0]);
      user.put("office", elements[1]);
      user.put("favorite_color", elements[2]);
      context.write(null, user);
    }
  }

  public int run(String[] args) throws Exception {

    if (args.length < 3) {
      System.err.println(
        "Usage: WriteParquet <input path> <output path> <schema path>"
      );
      return -1;
    }
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    String schemaPathName = args[2];

    Configuration conf = getConf();
    conf.set(SCHEMA_PATH_KEY, schemaPathName);
    Schema schema = getSchema(conf);

    Job job = new Job(conf);
    job.setJarByClass(getClass());
    job.setJobName(getClass().getName());

    AvroParquetOutputFormat.setSchema(job, schema);

    job.setMapperClass(WriteUserMap.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(AvroParquetOutputFormat.class);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    try {
      int res = ToolRunner.run(new Configuration(),
                               new WriteParquet(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(255);
    }
  }
}
