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

package it.crs4.pydoop.mapreduce.pipes;

import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.StringTokenizer;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * A command line parser for the CLI-based Pipes job submitter.
 */
class CommandLineParser {
    private Options options = new Options();

    CommandLineParser() {
        addOption("input", false, "input path to the maps", "path");
        addOption("output", false, "output path from the reduces", "path");
        addOption("jar", false, "job jar file", "path");
        addOption("inputformat", false, "java classname of InputFormat", 
                  "class");
        addOption("map", false, "java classname of Mapper", "class");
        addOption("partitioner", false, "java classname of Partitioner", 
                  "class");
        addOption("reduce", false, "java classname of Reducer", "class");
        addOption("writer", false, "java classname of OutputFormat", "class");
        addOption("program", false, "URI to application executable", "class");
        addOption("reduces", false, "number of reduces", "num");
        addOption("lazyOutput", false, "Optional. Create output lazily",
                  "boolean");
    }

    void addOption(String longName, boolean required, String description, 
                   String paramName) {
        Option option = OptionBuilder.withArgName(paramName)
                           .hasArgs(1).withDescription(description)
                               .isRequired(required).create(longName);
        options.addOption(option);
    }
    
    void addArgument(String name, boolean required, String description) {
        Option option = OptionBuilder.withArgName(name)
                           .hasArgs(1).withDescription(description)
                               .isRequired(required).create();
        options.addOption(option);
    }

    CommandLine parse(Configuration conf, String[] args) 
        throws IOException, ParseException {
        Parser parser = new BasicParser();
        conf.setBoolean("mapreduce.client.genericoptionsparser.used", true);
        GenericOptionsParser genericParser = new GenericOptionsParser(conf, args);
        return parser.parse(options, genericParser.getRemainingArgs());
    }

    void printUsage() {
      // The CLI package should do this for us, but I can't figure out how
      // to make it print something reasonable.
      System.out.println("bin/hadoop pipes");
      System.out.println("  [-input <path>] // Input directory");
      System.out.println("  [-output <path>] // Output directory");
      System.out.println("  [-jar <jar file> // jar filename");
      System.out.println("  [-inputformat <class>] // InputFormat class");
      System.out.println("  [-map <class>] // Java Map class");
      System.out.println("  [-partitioner <class>] // Java Partitioner");
      System.out.println("  [-reduce <class>] // Java Reduce class");
      System.out.println("  [-writer <class>] // Java RecordWriter");
      System.out.println("  [-program <executable>] // executable URI");
      System.out.println("  [-reduces <num>] // number of reduces");
      System.out.println("  [-lazyOutput <true/false>] // createOutputLazily");
      System.out.println();
      GenericOptionsParser.printGenericCommandUsage(System.out);
    }
  }



public class Submitter extends Configured implements Tool {

    protected static final Log LOG = LogFactory.getLog(Submitter.class);
    public static final String PRESERVE_COMMANDFILE = 
        "mapreduce.pipes.commandfile.preserve";
    public static final String EXECUTABLE = "mapreduce.pipes.executable";
    public static final String INTERPRETOR = 
        "mapreduce.pipes.executable.interpretor";
    public static final String IS_JAVA_MAP = "mapreduce.pipes.isjavamapper";
    public static final String IS_JAVA_RR = 
        "mapreduce.pipes.isjavarecordreader";
    public static final String IS_JAVA_RW = 
        "mapreduce.pipes.isjavarecordwriter";
    public static final String IS_JAVA_REDUCE = 
        "mapreduce.pipes.isjavareducer";
    public static final String PARTITIONER = "mapreduce.pipes.partitioner";
    public static final String INPUT_FORMAT = "mapreduce.pipes.inputformat";
    public static final String PORT = "mapreduce.pipes.command.port";

    
    /**
     * Get the URI of the application's executable.
     * @param conf
     * @return the URI where the application's executable is located
     */
    public static String getExecutable(Configuration conf) {
        return conf.get(Submitter.EXECUTABLE);
    }

    /**
     * Set the URI for the application's executable. Normally this is a hdfs: 
     * location.
     * @param conf
     * @param executable The URI of the application's executable.
     */
    public static void setExecutable(Configuration conf, String executable) {
        conf.set(Submitter.EXECUTABLE, executable);
    }

    /**
     * Set whether the job is using a Java RecordReader.
     * @param conf the configuration to modify
     * @param value the new value
     */
    public static void setIsJavaRecordReader(Configuration conf, boolean value) {
        conf.setBoolean(Submitter.IS_JAVA_RR, value);
    }

    /**
     * Check whether the job is using a Java RecordReader
     * @param conf the configuration to check
     * @return is it a Java RecordReader?
     */
    public static boolean getIsJavaRecordReader(Configuration conf) {
        return conf.getBoolean(Submitter.IS_JAVA_RR, false);
    }

    /**
     * Set whether the Mapper is written in Java.
     * @param conf the configuration to modify
     * @param value the new value
     */
    public static void setIsJavaMapper(Configuration conf, boolean value) {
        conf.setBoolean(Submitter.IS_JAVA_MAP, value);
    }

    /**
     * Check whether the job is using a Java Mapper.
     * @param conf the configuration to check
     * @return is it a Java Mapper?
     */
    public static boolean getIsJavaMapper(Configuration conf) {
        return conf.getBoolean(Submitter.IS_JAVA_MAP, false);
    }

    /**
     * Set whether the Reducer is written in Java.
     * @param conf the configuration to modify
     * @param value the new value
     */
    public static void setIsJavaReducer(Configuration conf, boolean value) {
        conf.setBoolean(Submitter.IS_JAVA_REDUCE, value);
    }

    /**
     * Check whether the job is using a Java Reducer.
     * @param conf the configuration to check
     * @return is it a Java Reducer?
     */
    public static boolean getIsJavaReducer(Configuration conf) {
        return conf.getBoolean(Submitter.IS_JAVA_REDUCE, false);
    }

    /**
     * Set whether the job will use a Java RecordWriter.
     * @param conf the configuration to modify
     * @param value the new value to set
     */
    public static void setIsJavaRecordWriter(Configuration conf, boolean value) {
        conf.setBoolean(Submitter.IS_JAVA_RW, value);
    }

    /**
     * Will the reduce use a Java RecordWriter?
     * @param conf the configuration to check
     * @return true, if the output of the job will be written by Java
     */
    public static boolean getIsJavaRecordWriter(Configuration conf) {
        return conf.getBoolean(Submitter.IS_JAVA_RW, false);
    }

    /**
     * Set the configuration, if it doesn't already have a value for the given
     * key.
     * @param conf the configuration to modify
     * @param key the key to set
     * @param value the new "default" value to set
     */
    private static void setIfUnset(Configuration conf, String key, String value) {
        if (conf.get(key) == null) {
            conf.set(key, value);
        }
    }

    /**
     * Save away the user's original partitioner before we override it.
     * @param conf the configuration to modify
     * @param cls the user's partitioner class
     */
    static void setJavaPartitioner(Configuration conf, Class cls) {
        conf.set(Submitter.PARTITIONER, cls.getName());
    }
  
    /**
     * Get the user's original partitioner.
     * @param conf the configuration to look in
     * @return the class that the user submitted
     */
    static Class<? extends Partitioner> getJavaPartitioner(Configuration conf) {
        return conf.getClass(Submitter.PARTITIONER, 
                             HashPartitioner.class,
                             Partitioner.class);
    }

    private static <InterfaceType> 
    Class<? extends InterfaceType> getClass(CommandLine cl, String key, 
                                            Configuration conf, 
                                            Class<InterfaceType> cls
                                            ) throws ClassNotFoundException {
        return conf.getClassByName(cl.getOptionValue(key)).asSubclass(cls);
    }
    /**
     * Does the user want to keep the command file for debugging? If this is
     * true, pipes will write a copy of the command data to a file in the
     * task directory named "downlink.data", which may be used to run the C++
     * program under the debugger. You probably also want to set 
     * Configuration.setKeepFailedTaskFiles(true) to keep the entire directory from
     * being deleted.
     * To run using the data file, set the environment variable 
     * "mapreduce.pipes.commandfile" to point to the file.
     * @param conf the configuration to check
     * @return will the framework save the command file?
     */
    public static boolean getKeepCommandFile(Configuration conf) {
        return conf.getBoolean(Submitter.PRESERVE_COMMANDFILE, false);
    }

    /**
     * Set whether to keep the command file for debugging
     * @param conf the configuration to modify
     * @param keep the new value
     */
    public static void setKeepCommandFile(Configuration conf, boolean keep) {
        conf.setBoolean(Submitter.PRESERVE_COMMANDFILE, keep);
    }

    private static void setupPipesJob(Job job) throws IOException, ClassNotFoundException {
        Configuration conf = job.getConfiguration();
        // default map output types to Text
        if (!getIsJavaMapper(conf)) {
            job.setMapperClass(PipesMapper.class);
            // Save the user's partitioner and hook in our's.
            setJavaPartitioner(conf, job.getPartitionerClass());
            job.setPartitionerClass(PipesPartitioner.class);
        }
        if (!getIsJavaReducer(conf)) {
            job.setReducerClass(PipesReducer.class);
            if (!getIsJavaRecordWriter(conf)) {
                job.setOutputFormatClass(NullOutputFormat.class);
            }
        }

        String textClassname = Text.class.getName();
        setIfUnset(conf, MRJobConfig.MAP_OUTPUT_KEY_CLASS, textClassname);
        setIfUnset(conf, MRJobConfig.MAP_OUTPUT_VALUE_CLASS, textClassname);
        setIfUnset(conf, MRJobConfig.OUTPUT_KEY_CLASS, textClassname);
        setIfUnset(conf, MRJobConfig.OUTPUT_VALUE_CLASS, textClassname);
    
        // Use PipesNonJavaInputFormat if necessary to handle progress reporting
        // from C++ RecordReaders ...
        if (!getIsJavaRecordReader(conf) && !getIsJavaMapper(conf)) {
            conf.setClass(Submitter.INPUT_FORMAT, 
                          job.getInputFormatClass(), InputFormat.class);
            job.setInputFormatClass(PipesNonJavaInputFormat.class);
        }
    
        String exec = getExecutable(conf);
        if (exec == null) {
            String msg = "No application program defined.";
            throw new IllegalArgumentException(msg);
        }
        // add default debug script only when executable is expressed as
        // <path>#<executable> 
        //FIXME: this is kind of useless if the pipes program is not in c++
        if (exec.contains("#")) {
            // set default gdb commands for map and reduce task 
            String defScript = "$HADOOP_PREFIX/src/c++/pipes/debug/pipes-default-script";
            setIfUnset(conf, MRJobConfig.MAP_DEBUG_SCRIPT,defScript);
            setIfUnset(conf, MRJobConfig.REDUCE_DEBUG_SCRIPT,defScript);
        }
        URI[] fileCache = DistributedCache.getCacheFiles(conf);
        if (fileCache == null) {
            fileCache = new URI[1];
        } else {
            URI[] tmp = new URI[fileCache.length+1];
            System.arraycopy(fileCache, 0, tmp, 1, fileCache.length);
            fileCache = tmp;
        }
        try {
            fileCache[0] = new URI(exec);
        } catch (URISyntaxException e) {
            String msg = "Problem parsing executable URI " + exec;
            IOException ie = new IOException(msg);
            ie.initCause(e);
            throw ie;
        }
        DistributedCache.setCacheFiles(fileCache, conf);
    }
    
    public int run(String[] args) throws Exception {
        CommandLineParser cli = new CommandLineParser();
        
        if (args.length == 0) {
            cli.printUsage();
            return 1;
        }

        try {
            Job job = new Job(new Configuration());
            job.setJobName(getClass().getName());
            Configuration conf = job.getConfiguration();

            CommandLine results = cli.parse(conf, args);

            if (results.hasOption("input")) {
                Path path = new Path(results.getOptionValue("input"));
                FileInputFormat.setInputPaths(job, path);
            }
            if (results.hasOption("output")) {
                Path path = new Path(results.getOptionValue("output"));
                FileOutputFormat.setOutputPath(job,path); 
            }
            if (results.hasOption("jar")) {
                job.setJar(results.getOptionValue("jar"));
            }
            if (results.hasOption("inputformat")) {
                setIsJavaRecordReader(conf, true);
                job.setInputFormatClass(getClass(results, "inputformat", conf,
                                                 InputFormat.class));
            }
            if (results.hasOption("javareader")) {
                setIsJavaRecordReader(conf, true);
            }
            if (results.hasOption("map")) {
                setIsJavaMapper(conf, true);
                job.setMapperClass(getClass(results, "map", conf, Mapper.class));
            }
            if (results.hasOption("partitioner")) {
                job.setPartitionerClass(getClass(results, "partitioner", conf,
                                                      Partitioner.class));
            }
            if (results.hasOption("reduce")) {
                setIsJavaReducer(conf, true);
                job.setReducerClass(
                            getClass(results, "reduce", conf, Reducer.class));
            }
            if (results.hasOption("reduces")) {
                job.setNumReduceTasks(
                     Integer.parseInt(results.getOptionValue("reduces")));
            }
            if (results.hasOption("writer")) {
                setIsJavaRecordWriter(conf, true);
                job.setOutputFormatClass(getClass(results, "writer", conf,
                                                  OutputFormat.class));
            }
            if (results.hasOption("lazyOutput")) {
                if (Boolean.parseBoolean(results.getOptionValue("lazyOutput"))) {
                    LazyOutputFormat.setOutputFormatClass(job,
                                                          job.getOutputFormatClass());
                }
            }
            if (results.hasOption("program")) {
                setExecutable(conf, results.getOptionValue("program"));
            }
            // if they gave us a jar file, include it into the class path
            String jarFile = job.getJar();
            if (jarFile != null) {
                final URL[] urls = 
                    new URL[]{ FileSystem.getLocal(conf).
                               pathToFile(new Path(jarFile)).toURL()};
                //FindBugs complains that creating a URLClassLoader should be
                //in a doPrivileged() block. 
                ClassLoader loader =
                    AccessController.doPrivileged(
                           new PrivilegedAction<ClassLoader>() {
                               public ClassLoader run() {
                                   return new URLClassLoader(urls);
                               }
                           }
                                                  );
                conf.setClassLoader(loader);
            }
            setupPipesJob(job);
            return job.waitForCompletion(true)? 0:1;
        } catch (ParseException pe) {
            LOG.info("Error : " + pe);
            cli.printUsage();
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode =  new Submitter().run(args);
        ExitUtil.terminate(exitCode);
    }
}
