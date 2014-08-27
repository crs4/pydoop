/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.crs4.pydoop.pipes;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * An adaptor to run an external mapper.
 */
class PipesMapper<K1 extends WritableComparable, V1 extends Writable,
                  K2 extends WritableComparable, V2 extends Writable>
    extends Mapper<K1, V1, K2, V2> {

    MapContext context;
    Application<K1, V1, K2, V2> application = null;
    boolean skipping = false;

    /**
     * Get the new configuration.
     * @param job the job's configuration
     */
    protected void setup(MapContext context) 
        throws IOException, InterruptedException {
        this.context = context;
        //disable the auto increment of the counter. For pipes, no of processed 
        //records could be different(equal or less) than the no of records input.
        // FIXME: disable right now...
        // SkipBadRecords.setAutoIncrMapperProcCount(context, false);
    }
    protected void cleanup(MapContext context
                           ) throws IOException, InterruptedException {
        if (application != null)  {
            application.cleanup();
        }
    }

    /**
     * Run the map task.
     */
    @SuppressWarnings("unchecked")
    public void run(MapContext<K1,V1,K2,V2> context) 
        throws IOException, InterruptedException {
        
        setup(context);

        Configuration conf = context.getConfiguration();
        InputSplit split = context.getInputSplit();

        // FIXME: do we really need to be so convoluted?
        
        InputFormat<K1, V1> inputFormat;
        try {
             inputFormat = (InputFormat<K1, V1>) 
                 ReflectionUtils.newInstance(context.getInputFormatClass(), conf);
        } catch (ClassNotFoundException ce) {
            throw new RuntimeException("class not found", ce);
        }

        RecordReader<K1, V1> input = inputFormat.createRecordReader(split, context);

        boolean isJavaInput = Submitter.getIsJavaRecordReader(conf);
        
        try {
            // FIXME: what happens for a java mapper and no jave record reader?
            DummyRecordReader fakeInput = 
                (!isJavaInput && !Submitter.getIsJavaMapper(conf)) ?
                (DummyRecordReader) input : null;
            application = new Application<K1, V1, K2, V2>(context, fakeInput);
        } catch (InterruptedException ie) {
            throw new RuntimeException("interrupted", ie);
        }
        DownwardProtocol<K1, V1> downlink = application.getDownlink();
        // FIXME: InputSplit is not Writable, but still, this is ugly...
        downlink.runMap((FileSplit) context.getInputSplit(), context.getNumReduceTasks(), 
                        isJavaInput);
        boolean skipping = conf.getBoolean(context.SKIP_RECORDS, false);
        try {
            if (isJavaInput) {
                K1 key = input.getCurrentKey();
                V1 value = input.getCurrentValue();
                downlink.setInputTypes(key.getClass().getName(), 
                                       value.getClass().getName());
                while (input.nextKeyValue()) {
                    downlink.mapItem(input.getCurrentKey(), input.getCurrentValue());
                    if(skipping) {
                        //flush the streams on every record input if running in skip mode
                        //so that we don't buffer other records surrounding a bad record.
                        downlink.flush();
                    }
                }
                downlink.endOfInput();
            }
            application.waitForFinish();
        } catch (Throwable t) {
            application.abort(t);
        } finally {
            cleanup(context);
        }
    }
}
