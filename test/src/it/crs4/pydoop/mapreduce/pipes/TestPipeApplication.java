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

//package org.apache.hadoop.mapred.pipes;

package it.crs4.pydoop.mapreduce.pipes;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;



import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.task.TaskInputOutputContextImpl;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestPipeApplication {
    private static File workSpace = new File("target",
                         TestPipeApplication.class.getName() + "-workSpace");

    private static String taskName = "attempt_001_02_r03_04_05";

    /**
     * test PipesMapRunner    test the transfer data from reader
     *
     * @throws Exception
     */
    @Test
    public void testRunner() throws Exception {
        // clean old password files
        File[] psw = cleanTokenPasswordFile();
        try {
            JobID jobId = new JobID("201408272347", 0);
            TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
            TaskAttemptID taskAttemptid = new TaskAttemptID(taskId, 0);

            Job job = new Job(new Configuration());
            job.setJobID(jobId);
            Configuration conf = job.getConfiguration();
            conf.set(Submitter.IS_JAVA_RR, "true");
            conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttemptid.toString());
            job.setInputFormatClass(DummyInputFormat.class);
            FileSystem fs = new RawLocalFileSystem();
            fs.setConf(conf);

            DummyInputFormat input_format = new DummyInputFormat();
            List<InputSplit> isplits = input_format.getSplits(job);
            
            InputSplit isplit = isplits.get(0);

            TaskAttemptContextImpl tcontext = 
                new TaskAttemptContextImpl(conf, taskAttemptid);

            RecordReader<FloatWritable, NullWritable> rReader = 
                input_format.createRecordReader(isplit, tcontext);


            TestMapContext context = new TestMapContext(conf, taskAttemptid, rReader, 
                                                        null, null, null, isplit);
            // stub for client
            File fCommand = getFileCommand("it.crs4.pydoop.mapreduce.pipes.PipeApplicationRunnableStub");
            conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());
            // token for authorization
            Token<AMRMTokenIdentifier> token = 
                new Token<AMRMTokenIdentifier>("user".getBytes(), 
                                               "password".getBytes(), 
                                               new Text("kind"), 
                                               new Text("service"));
            TokenCache.setJobToken(token,  job.getCredentials());
            conf.setBoolean(MRJobConfig.SKIP_RECORDS, true);
            PipesMapper<FloatWritable, NullWritable, IntWritable, Text> mapper = 
                new PipesMapper<FloatWritable, NullWritable, IntWritable, Text>(context);

            initStdOut(conf);
            mapper.run(context);
            String stdOut = readStdOut(conf);

            // test part of translated data. As common file for client and test -
            // clients stdOut
            // check version
            assertTrue(stdOut.contains("CURRENT_PROTOCOL_VERSION:0"));
            // check key and value classes
            assertTrue(stdOut
                       .contains("Key class:org.apache.hadoop.io.FloatWritable"));
            assertTrue(stdOut
                       .contains("Value class:org.apache.hadoop.io.NullWritable"));
            // test have sent all data from reader
            assertTrue(stdOut.contains("value:0.0"));
            assertTrue(stdOut.contains("value:9.0"));

        } finally {
            if (psw != null) {
                // remove password files
                for (File file : psw) {
                    file.deleteOnExit();
                }
            }
        }
    }

    /**
     * test org.apache.hadoop.mapreduce.pipes.Application
     * test a internal functions: 
     *     MessageType.REGISTER_COUNTER,  INCREMENT_COUNTER, STATUS, PROGRESS...
     *
     * @throws Throwable
     */

    @Test
    public void testApplication() throws Throwable {

        System.err.println("testApplication");

        File[] psw = cleanTokenPasswordFile();
        try {
            JobID jobId = new JobID("201408272347", 0);
            TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
            TaskAttemptID taskAttemptid = new TaskAttemptID(taskId, 0);

            Job job = new Job(new Configuration());
            job.setJobID(jobId);
            Configuration conf = job.getConfiguration();
            conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttemptid.toString());
            FileSystem fs = new RawLocalFileSystem();
            fs.setConf(conf);

            File fCommand = 
                getFileCommand("it.crs4.pydoop.mapreduce.pipes.PipeApplicationStub");
                //getFileCommand("it.crs4.pydoop.mapreduce.pipes.PipeApplicationRunnableStub");
            conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());            
            System.err.println("fCommand" + fCommand.getAbsolutePath());

            Token<AMRMTokenIdentifier> token = 
                new Token<AMRMTokenIdentifier>("user".getBytes(), 
                                               "password".getBytes(), 
                                               new Text("kind"), 
                                               new Text("service"));
            TokenCache.setJobToken(token,  job.getCredentials());
            conf.setBoolean(MRJobConfig.SKIP_RECORDS, true);

            TestReporter reporter = new TestReporter();
            DummyInputFormat input_format = new DummyInputFormat();
            List<InputSplit> isplits = input_format.getSplits(job);
            InputSplit isplit = isplits.get(0);
            TaskAttemptContextImpl tcontext = 
                new TaskAttemptContextImpl(conf, taskAttemptid);

            DummyRecordReader reader =  
                (DummyRecordReader) input_format.createRecordReader(isplit, tcontext);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            
            RecordWriter<IntWritable, Text>  writer = 
                new TestRecordWriter(new FileOutputStream( 
                    workSpace.getAbsolutePath() + File.separator + "outfile"));

            MapContextImpl<IntWritable, Text, 
                           IntWritable, Text> context = 
                new MapContextImpl<IntWritable, Text, 
                                   IntWritable, Text>(
                      conf, taskAttemptid, null, writer, null, reporter, null);

            System.err.println("ready to launch application");
            Application<IntWritable, Text, 
                        IntWritable, Text> application = 
                new Application<IntWritable, Text, 
                                IntWritable, Text>(context, reader);
            System.err.println("done");
            
            application.getDownlink().flush();
            application.getDownlink().mapItem(new IntWritable(3), new Text("txt"));
            application.getDownlink().flush();
            application.waitForFinish();

            // test getDownlink().mapItem();
            String stdOut = readStdOut(conf);
            assertTrue(stdOut.contains("key:3"));
            assertTrue(stdOut.contains("value:txt"));

            assertEquals(0.0, context.getProgress(), 0.01);
            assertNotNull(context.getCounter("group", "name"));

            // test status MessageType.STATUS
            assertEquals(context.getStatus(), "PROGRESS");
            // check MessageType.PROGRESS
            assertEquals(0.55f, reader.getProgress(), 0.001);
            application.getDownlink().close();
            // test MessageType.OUTPUT
            stdOut = readFile(new File(workSpace.getAbsolutePath() + File.separator
                                       + "outfile"));
            assertTrue(stdOut.contains("key:123"));
            assertTrue(stdOut.contains("value:value"));
            try {
                // try to abort
                application.abort(new Throwable());
                fail();
            } catch (IOException e) {
                // abort works ?
                assertEquals("pipe child exception", e.getMessage());
            }
        } finally {
            if (psw != null) {
                // remove password files
                for (File file : psw) {
                    file.deleteOnExit();
                }
            }
        }
    }

    /**
     * test org.apache.hadoop.mapreduce.pipes.Submitter
     *
     * @throws Exception
     */
    @Test
    public void testSubmitter() throws Exception {

        Configuration conf = new Configuration();

        File[] psw = cleanTokenPasswordFile();

        System.setProperty("test.build.data",
                           "target/tmp/build/TEST_SUBMITTER_MAPPER/data");
        conf.set("hadoop.log.dir", "target/tmp");

        // prepare configuration
        Submitter.setIsJavaMapper(conf, false);
        Submitter.setIsJavaReducer(conf, false);
        Submitter.setKeepCommandFile(conf, false);
        Submitter.setIsJavaRecordReader(conf, false);
        Submitter.setIsJavaRecordWriter(conf, false);
        PipesPartitioner<IntWritable, Text> partitioner = 
            new PipesPartitioner<IntWritable, Text>();
        partitioner.configure(conf);

        Submitter.setJavaPartitioner(conf, partitioner.getClass());

        assertEquals(PipesPartitioner.class, (Submitter.getJavaPartitioner(conf)));
        // test going to call main method with System.exit(). Change Security
        SecurityManager securityManager = System.getSecurityManager();
        // store System.out
        PrintStream oldps = System.out;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ExitUtil.disableSystemExit();
        // test without parameters
        try {
            System.setOut(new PrintStream(out));
            Submitter.main(new String[0]);
            fail();
        } catch (ExitUtil.ExitException e) {
            // System.exit prohibited! output message test
            assertTrue(out.toString().contains(""));
            assertTrue(out.toString().contains("bin/hadoop pipes"));
            assertTrue(out.toString().contains("[-input <path>] // Input directory"));
            assertTrue(out.toString()
                       .contains("[-output <path>] // Output directory"));
            assertTrue(out.toString().contains("[-jar <jar file> // jar filename"));
            assertTrue(out.toString().contains(
                                               "[-inputformat <class>] // InputFormat class"));
            assertTrue(out.toString().contains("[-map <class>] // Java Map class"));
            assertTrue(out.toString().contains(
                                               "[-partitioner <class>] // Java Partitioner"));
            assertTrue(out.toString().contains(
                                               "[-reduce <class>] // Java Reduce class"));
            assertTrue(out.toString().contains(
                                               "[-writer <class>] // Java RecordWriter"));
            assertTrue(out.toString().contains(
                                               "[-program <executable>] // executable URI"));
            assertTrue(out.toString().contains(
                                               "[-reduces <num>] // number of reduces"));
            assertTrue(out.toString().contains(
                                               "[-lazyOutput <true/false>] // createOutputLazily"));

            assertTrue(out
                       .toString()
                       .contains(
                                 "-conf <configuration file>     specify an application configuration file"));
            assertTrue(out.toString().contains(
                                               "-D <property=value>            use value for given property"));
            assertTrue(out.toString().contains(
                                               "-fs <local|namenode:port>      specify a namenode"));
            assertTrue(out.toString().contains(
                                               "-jt <local|jobtracker:port>    specify a job tracker"));
            assertTrue(out
                       .toString()
                       .contains(
                                 "-files <comma separated list of files>    specify comma separated files to be copied to the map reduce cluster"));
            assertTrue(out
                       .toString()
                       .contains(
                                 "-libjars <comma separated list of jars>    specify comma separated jar files to include in the classpath."));
            assertTrue(out
                       .toString()
                       .contains(
                                 "-archives <comma separated list of archives>    specify comma separated archives to be unarchived on the compute machines."));
        } finally {
            System.setOut(oldps);
            // restore
            System.setSecurityManager(securityManager);
            if (psw != null) {
                // remove password files
                for (File file : psw) {
                    file.deleteOnExit();
                }
            }
        }
        // test call Submitter form command line
        try {
            File fCommand = getFileCommand(null);
            String[] args = new String[20];
            File input = new File(workSpace + File.separator + "input");
            if (!input.exists()) {
                Assert.assertTrue(input.createNewFile());
            }
            File outPut = new File(workSpace + File.separator + "output");
            FileUtil.fullyDelete(outPut);

            args[0] = "-input";
            args[1] = input.getAbsolutePath();// "input";
            args[2] = "-output";
            args[3] = outPut.getAbsolutePath();// "output";
            args[4] = "-inputformat";
            args[5] = "org.apache.hadoop.mapreduce.lib.input.TextInputFormat";
            args[6] = "-map";
            args[7] = "org.apache.hadoop.mapreduce.lib.map.InverseMapper";
            args[8] = "-partitioner";
            args[9] = "it.crs4.pydoop.mapreduce.pipes.PipesPartitioner";
            args[10] = "-reduce";
            args[11] = "org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer";
            args[12] = "-writer";
            args[13] = "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat";
            args[14] = "-program";
            args[15] = fCommand.getAbsolutePath();// "program";
            args[16] = "-reduces";
            args[17] = "2";
            args[18] = "-lazyOutput";
            args[19] = "lazyOutput";
            Submitter.main(args);
            fail();
        } catch (ExitUtil.ExitException e) {
            // status should be 0
            assertEquals(e.status, 0);

        } finally {
            System.setOut(oldps);
            System.setSecurityManager(securityManager);
        }

    }

    /**
     * test org.apache.hadoop.mapreduce.pipes.PipesReducer
     * test the transfer of data: key and value
     *
     * @throws Exception
     */
    @Test
    public void testPipesReducer() throws Exception {
        System.err.println("testPipesReducer");

        File[] psw = cleanTokenPasswordFile();
        try {
            JobID jobId = new JobID("201408272347", 0);
            TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);
            TaskAttemptID taskAttemptid = new TaskAttemptID(taskId, 0);

            Job job = new Job(new Configuration());
            job.setJobID(jobId);
            Configuration conf = job.getConfiguration();
            conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskAttemptid.toString());
            FileSystem fs = new RawLocalFileSystem();
            fs.setConf(conf);

            File fCommand = getFileCommand("it.crs4.pydoop.mapreduce.pipes.PipeReducerStub");
            conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());
            System.err.println("fCommand" + fCommand.getAbsolutePath());

            Token<AMRMTokenIdentifier> token = 
                new Token<AMRMTokenIdentifier>("user".getBytes(), 
                                               "password".getBytes(), 
                                               new Text("kind"), 
                                               new Text("service"));
            TokenCache.setJobToken(token, job.getCredentials());
            conf.setBoolean(MRJobConfig.SKIP_RECORDS, true);

            TestReporter reporter = new TestReporter();
            DummyInputFormat input_format = new DummyInputFormat();
            List<InputSplit> isplits = input_format.getSplits(job);
            InputSplit isplit = isplits.get(0);
            TaskAttemptContextImpl tcontext = 
                new TaskAttemptContextImpl(conf, taskAttemptid);

            RecordWriter<IntWritable, Text>  writer = 
                new TestRecordWriter(new FileOutputStream( 
                    workSpace.getAbsolutePath() + File.separator + "outfile"));

            BooleanWritable bw = new BooleanWritable(true);
            List<Text> texts = new ArrayList<Text>();
            texts.add(new Text("first"));
            texts.add(new Text("second"));
            texts.add(new Text("third"));

            DummyRawKeyValueIterator kvit = new DummyRawKeyValueIterator();

            ReduceContextImpl<BooleanWritable, Text, IntWritable, Text> context 
                = new ReduceContextImpl<
                BooleanWritable, Text, IntWritable, Text>(
                      conf, taskAttemptid, kvit,
                      null, null, writer, null, null, null, 
                      BooleanWritable.class, Text.class);

            PipesReducer<BooleanWritable, Text, IntWritable, Text> 
                reducer = new PipesReducer<BooleanWritable, Text, IntWritable, Text>();
            reducer.setup(context);

            initStdOut(conf);
            reducer.reduce(bw, texts, context);
            reducer.cleanup(context);
            String stdOut = readStdOut(conf);

            // test data: key
            assertTrue(stdOut.contains("reducer key :true"));
            // and values
            assertTrue(stdOut.contains("reduce value  :first"));
            assertTrue(stdOut.contains("reduce value  :second"));
            assertTrue(stdOut.contains("reduce value  :third"));

        } finally {
            if (psw != null) {
                // remove password files
                for (File file : psw) {
                    file.deleteOnExit();
                }
            }
        }

    }

    /**
     * test PipesPartitioner
     * test set and get data from  PipesPartitioner
     */
    @Test
    public void testPipesPartitioner() {

        PipesPartitioner<IntWritable, Text> partitioner = 
            new PipesPartitioner<IntWritable, Text>();
        Configuration configuration = new Configuration();
        Submitter.getJavaPartitioner(configuration);
        partitioner.configure(new Configuration());
        IntWritable iw = new IntWritable(4);
        // the cache empty
        assertEquals(0, partitioner.getPartition(iw, new Text("test"), 2));
        // set data into cache
        PipesPartitioner.setNextPartition(3);
        // get data from cache
        assertEquals(3, partitioner.getPartition(iw, new Text("test"), 2));
    }

    /**
     * clean previous std error and outs
     */

    private void initStdOut(Configuration configuration) {
        TaskAttemptID taskId = TaskAttemptID.forName(configuration
                                                     .get(MRJobConfig.TASK_ATTEMPT_ID));
        File stdOut = TaskLog.getTaskLogFile(taskId, false, TaskLog.LogName.STDOUT);
        File stdErr = TaskLog.getTaskLogFile(taskId, false, TaskLog.LogName.STDERR);
        // prepare folder
        if (!stdOut.getParentFile().exists()) {
            stdOut.getParentFile().mkdirs();
        } else { // clean logs
            stdOut.deleteOnExit();
            stdErr.deleteOnExit();
        }
    }

    private String readStdOut(Configuration conf) throws Exception {
        TaskAttemptID taskId = TaskAttemptID.forName(conf
                                                     .get(MRJobConfig.TASK_ATTEMPT_ID));
        File stdOut = TaskLog.getTaskLogFile(taskId, false, TaskLog.LogName.STDOUT);

        return readFile(stdOut);

    }

    private String readFile(File file) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int counter = 0;
        while ((counter = is.read(buffer)) >= 0) {
            out.write(buffer, 0, counter);
        }

        is.close();

        return out.toString();

    }

    private class Progress implements Progressable {

        @Override
        public void progress() {

        }

    }

    private File[] cleanTokenPasswordFile() throws Exception {
        File[] result = new File[2];
        result[0] = new File("./jobTokenPassword");
        if (result[0].exists()) {
            FileUtil.chmod(result[0].getAbsolutePath(), "700");
            assertTrue(result[0].delete());
        }
        result[1] = new File("./.jobTokenPassword.crc");
        if (result[1].exists()) {
            FileUtil.chmod(result[1].getAbsolutePath(), "700");
            result[1].delete();
        }
        return result;
    }

    private File getFileCommand(String clazz) throws Exception {
        String classpath = System.getProperty("java.class.path");
        File fCommand = new File(workSpace + File.separator + "cache.sh");
        fCommand.deleteOnExit();
        if (!fCommand.getParentFile().exists()) {
            fCommand.getParentFile().mkdirs();
        }
        fCommand.createNewFile();
        OutputStream os = new FileOutputStream(fCommand);
        os.write("#!/bin/sh \n".getBytes());
        if (clazz == null) {
            os.write(("ls ").getBytes());
        } else {
            os.write(("java -cp " + classpath + " " + clazz).getBytes());
        }
        os.flush();
        os.close();
        FileUtil.chmod(fCommand.getAbsolutePath(), "700");
        return fCommand;
    }

    private class TestRecordWriter extends  
        RecordWriter<IntWritable, Text> {
        private OutputStream os;

        public TestRecordWriter(OutputStream os) {
            this.os = os;
        }


        @Override
        public void write(IntWritable key, Text value) 
            throws IOException, InterruptedException {
            os.write(("key:" + key + "\n").getBytes());
            os.write(("value:" + value + "\n").getBytes());
        }
        @Override
        public void close(TaskAttemptContext context)
            throws IOException, InterruptedException {
            os.close();
        }
    }

    private class TestReporter extends StatusReporter  {
        private String status;
        private float progress;

        public Counter getCounter(Enum<?> name) {
            return new Counters().findCounter(name);
        }
        public Counter getCounter(String group, String name) {
            return new Counters().findCounter(group, name);
        }
        public void progress() {
        }
        public float getProgress() {
            progress = 0.0f;
            return progress;
        }
        public void setStatus(String status) {
        }
    }

    private class TestMapContext extends
        MapContextImpl<FloatWritable, NullWritable, IntWritable, Text>  {

        TestMapContext(Configuration conf, 
                       TaskAttemptID tid,
                       RecordReader<FloatWritable, NullWritable> reader,
                       RecordWriter<IntWritable, Text> writer,
                       OutputCommitter committer,
                       StatusReporter reporter,
                       InputSplit split)  {
            super(conf, tid, reader, writer, committer, reporter, split);
        }
    }

    private class DummyRawKeyValueIterator implements RawKeyValueIterator {
        public DataInputBuffer getKey() throws IOException { return null; }
        public DataInputBuffer getValue() throws IOException { return null;}
        public boolean next() throws IOException { return true; }
        public void close() throws IOException {}
        public org.apache.hadoop.util.Progress getProgress() { return null; }
    }
}
