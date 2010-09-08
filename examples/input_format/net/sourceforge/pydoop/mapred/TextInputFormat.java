// BEGIN_COPYRIGHT
// END_COPYRIGHT

package net.sourceforge.pydoop.mapred;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TextInputFormat extends FileInputFormat<LongWritable, Text>
    implements JobConfigurable {
    
    private Boolean will_split = true;

    public void configure(JobConf conf) {
	String can_split = conf.get("pydoop.input.issplitable");
	will_split = can_split.equals("true");
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
	return will_split;
    }
    
    public RecordReader<LongWritable, Text> getRecordReader(
                                          InputSplit genericSplit, JobConf job,
                                          Reporter reporter)
	throws IOException {
	reporter.setStatus(genericSplit.toString());
	return new LineRecordReader(job, (FileSplit) genericSplit);
    }
}
