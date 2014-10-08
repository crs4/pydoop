/*
  Based on Cloudera TestReadParquet example.
 */

package it.crs4.pydoop;

import static java.lang.Thread.sleep;
//import static org.junit.Assert.*;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.avro.io.EncoderFactory;


import parquet.Log;
import parquet.example.data.Group;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import parquet.hadoop.ParquetInputSplit;
//import parquet.hadoop.example.ExampleInputFormat;
import parquet.avro.AvroParquetInputFormat;

public class ExampleParquetMRReader extends Configured implements Tool {
    private static final Log LOG = Log.getLog(ExampleParquetMRReader.class);
    /*
     * Read an Avro record, write out its serialization
     */
    public static class ReadRequestMap 
        extends Mapper<LongWritable, Record, NullWritable, Text> {
        @Override
        public void map(LongWritable key, Record record, Context context) 
            throws IOException, InterruptedException {
            NullWritable outKey = NullWritable.get();
            Schema schema = record.getSchema();
            DatumWriter<GenericRecord> datumWriter = 
                new GenericDatumWriter<GenericRecord>(schema);
            EncoderFactory fact = EncoderFactory.get();

            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            BinaryEncoder enc = fact.binaryEncoder(stream, null);
            datumWriter.write(record, enc);
            enc.flush();
            context.write(outKey, new Text(stream.toByteArray()));
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ExampleParquetMRRead <input path> <output path>");
            return -1;
        }

        getConf().set("mapred.textoutputformat.separator", ",");

        Job job = new Job(getConf());
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ReadRequestMap.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), 
                                     new ExampleParquetMRReader(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }
}

