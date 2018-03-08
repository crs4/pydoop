package it.crs4.pydoop.mapreduce.pipes;

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

class OpaqueSplit extents InputSplit implements Writable {
    private BytesWritable code;
    private BytesWritable payload;

    public OpaqueSplit() {
        this.code = BytesWritable();
        this.payload = BytesWritable();
    }

    public OpaqueSplit(bytes[] code, bytes[] payload) {
        this.code = BytesWritable(code);
        this.payload = BytesWritable(payload);
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
