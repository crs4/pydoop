package it.crs4.pydoop.mapreduce.pipes;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;

public abstract class DummyRecordReader 
    extends RecordReader<FloatWritable, NullWritable> {

    public abstract  boolean next(FloatWritable key, NullWritable value)
        throws IOException ;
}
