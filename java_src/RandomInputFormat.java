package it.crs4.hadoop.pydoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.io.Text;


class RandomInputFormat implements InputFormat<Text, Text> {

    /** 
     * Generate the requested number of file splits, with the filename
     * set to the filename of the output file.
     */
    public InputSplit[] getSplits(JobConf job, 
                                  int numSplits) throws IOException {
	InputSplit[] result = new InputSplit[numSplits];
	Path outDir = FileOutputFormat.getOutputPath(job);
	for(int i=0; i < result.length; ++i) {
	    result[i] = new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, 
				      (String[])null);
	}
	return result;
    }

    /**
     * Return a single record (filename, "") where the filename is taken from
     * the file split.
     */
    static class RandomRecordReader implements RecordReader<Text, Text> {
	Path name;
	public RandomRecordReader(Path p) {
	    name = p;
	}
	public boolean next(Text key, Text value) {
	    if (name != null) {
		key.set(name.getName());
		name = null;
		return true;
	    }
	    return false;
	}
	public Text createKey() {
	    return new Text();
	}
	public Text createValue() {
	    return new Text();
	}
	public long getPos() {
	    return 0;
	}
	public void close() {}
	public float getProgress() {
	    return 0.0f;
	}
    }

    public RecordReader<Text, Text> getRecordReader(InputSplit split,
						    JobConf job, 
						    Reporter reporter) throws IOException {
	return new RandomRecordReader(((FileSplit) split).getPath());
    }
}
