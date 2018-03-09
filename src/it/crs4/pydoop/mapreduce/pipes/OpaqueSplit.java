package it.crs4.pydoop.mapreduce.pipes;

import org.apache.hadoop.mapred.SplitLocationInfo;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * A split used to opaquely transport generic split information to pydoop python
 * RecordReaders.
 */

class OpaqueSplit extends InputSplit implements Writable {
    private BytesWritable code;
    private BytesWritable payload;

    public OpaqueSplit() {
        this.code = new BytesWritable();
        this.payload = new BytesWritable();
    }

    public OpaqueSplit(byte[] code, byte[] payload) {
        this.code = new BytesWritable(code);
        this.payload = new BytesWritable(payload);
    }

    public BytesWritable getCode() {
        return this.code;
    }
    
    public BytesWritable getPayload() {
        return this.payload;
    }

    /** The number of bytes in the file to process. */
    @Override
    public long getLength() { return 0; } // FIXME

    @Override
    public String toString() { return ""; } // FIXME
    
    @Override
    public String[] getLocations() throws IOException { // FIXME
        return new String[]{};
    }
  
    @Override
    public SplitLocationInfo[] getLocationInfo() throws IOException {
        return new SplitLocationInfo[]{}; // FIXME
    }    

    ////////////////////////////////////////////
    // Writable methods
    ////////////////////////////////////////////
    @Override
    public void write(DataOutput out) throws IOException {
        this.code.write(out);
        this.payload.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.code.readFields(in);
        this.payload.readFields(in);
    }
}
