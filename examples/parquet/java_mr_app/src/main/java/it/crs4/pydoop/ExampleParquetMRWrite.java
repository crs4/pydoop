/*
  Based on Cloudera TestReadParquet example.
 */

package it.crs4.pydoop;

import static java.lang.Thread.sleep;
//import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import parquet.Log;
import parquet.example.data.Group;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;

import parquet.avro.AvroParquetOutputFormat;

public class ExampleParquetMRWrite extends Configured implements Tool {
    private static final Log LOG = Log.getLog(ExampleParquetMRWrite.class);

    private static final String USER_SCHEMA = "{\n\"namespace\": \"example.avro\",\n \"type\": \"record\",\n \"name\": \"User\",\n \"fields\": [\n     {\"name\": \"office\", \"type\": \"string\"},\n     {\"name\": \"name\", \"type\": \"string\"},\n     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n ]\n}";

    public static class WriteUserMap
        extends Mapper<LongWritable, Text, NullWritable, Record> {
        @Override
        public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
            
            // FIXME -- this is clearly not the way to do it.
            Schema schema = new Schema.Parser().parse(USER_SCHEMA);
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
        if (args.length != 3) {
            System.err.println("Usage: ExampleParquetMRWrite <input path> <output path>");
            return -1;
        }
        Schema schema = new Schema.Parser().parse(USER_SCHEMA);

        Configuration conf = getConf();
        Job job = new Job(conf);

        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        AvroParquetOutputFormat.setSchema(job, schema);

        job.setMapperClass(WriteUserMap.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);

        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), 
                                     new ExampleParquetMRWrite(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }
}

