// BEGIN_COPYRIGHT
// END_COPYRIGHT

package net.sourceforge.pydoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public class TextInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
						InputSplit split,
						TaskAttemptContext context) {
	return new LineRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
	return context.getConfiguration().getBoolean("pydoop.input.issplitable", true);
    }
}
