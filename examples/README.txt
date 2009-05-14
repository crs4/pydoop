1/ Are you sure that hdsf is still there?

$ hadoop namenode -format 
$ start-all.sh
$ hadoop fs -mkdir examples/bin
$ hadoop fs -put WordCount examples/bin

zag@manzanillo examples $ hadoop fs -ls  examples/bin
Found 1 items
-rw-r--r--   1 zag supergroup     160713 2009-05-14 10:19 /user/zag/examples/bin/WordCount

zag@manzanillo examples $ hadoop fs -put in-dir .
zag@manzanillo examples $ hadoop fs -ls              
Found 2 items
drwxr-xr-x   - zag supergroup          0 2009-05-14 10:18 /user/zag/examples
drwxr-xr-x   - zag supergroup          0 2009-05-14 10:20 /user/zag/in-dir

zag@manzanillo examples $ cat conf/word.xml 
<?xml version="1.0"?>
<configuration>
<property>
  <name>hadoop.pipes.executable</name>
  <!-- note the relative path -->
  <value>examples/bin/WordCount</value>
</property>
<property>
  <name>hadoop.pipes.java.recordreader</name>
  <value>true</value>
</property>
<property>
  <name>hadoop.pipes.java.recordwriter</name>
  <value>true</value>
</property>
<property>
  <name>keep.failed.task.files</name>
  <value>true</value>
</property>
<property>
  <name>hadoop.pipes.command-file.keep</name>
  <value>true</value>
</property>
</configuration>

zag@manzanillo examples $ hadoop pipes -conf conf/word.xml -input in-dir -output our-dir
09/05/14 10:23:04 WARN mapred.JobClient: No job jar file set.  User classes may not be found. See JobConf(Class) or JobConf#setJar(String).
09/05/14 10:23:04 INFO mapred.FileInputFormat: Total input paths to process : 1
09/05/14 10:23:04 INFO mapred.JobClient: Running job: job_200905141017_0001
09/05/14 10:23:05 INFO mapred.JobClient:  map 0% reduce 0%
09/05/14 10:23:12 INFO mapred.JobClient:  map 50% reduce 0%
09/05/14 10:23:17 INFO mapred.JobClient:  map 100% reduce 0%
09/05/14 10:23:22 INFO mapred.JobClient:  map 100% reduce 16%
09/05/14 10:23:27 INFO mapred.JobClient:  map 100% reduce 100%
09/05/14 10:23:28 INFO mapred.JobClient: Job complete: job_200905141017_0001
09/05/14 10:23:28 INFO mapred.JobClient: Counters: 16
09/05/14 10:23:28 INFO mapred.JobClient:   File Systems
09/05/14 10:23:28 INFO mapred.JobClient:     HDFS bytes read=1464
09/05/14 10:23:28 INFO mapred.JobClient:     HDFS bytes written=883
09/05/14 10:23:28 INFO mapred.JobClient:     Local bytes read=1184
09/05/14 10:23:28 INFO mapred.JobClient:     Local bytes written=2430
09/05/14 10:23:28 INFO mapred.JobClient:   Job Counters 
09/05/14 10:23:28 INFO mapred.JobClient:     Launched reduce tasks=1
09/05/14 10:23:28 INFO mapred.JobClient:     Launched map tasks=2
09/05/14 10:23:28 INFO mapred.JobClient:     Data-local map tasks=2
09/05/14 10:23:28 INFO mapred.JobClient:   Map-Reduce Framework
09/05/14 10:23:28 INFO mapred.JobClient:     Reduce input groups=51
09/05/14 10:23:28 INFO mapred.JobClient:     Combine output records=0
09/05/14 10:23:28 INFO mapred.JobClient:     Map input records=32
09/05/14 10:23:28 INFO mapred.JobClient:     Reduce output records=51
09/05/14 10:23:28 INFO mapred.JobClient:     Map output bytes=1030
09/05/14 10:23:28 INFO mapred.JobClient:     Map input bytes=975
09/05/14 10:23:28 INFO mapred.JobClient:     Combine input records=0
09/05/14 10:23:28 INFO mapred.JobClient:     Map output records=74
09/05/14 10:23:28 INFO mapred.JobClient:     Reduce input records=74

zag@manzanillo examples $ hadoop fs -put wordcount-py examples/bin

zag@manzanillo examples $ cat conf/word-python.xml 
<?xml version="1.0"?>
<configuration>
  <property>
    <name>keep.failed.task.files</name>
    <value>true</value>
  </property>
  <property>
    <name>hadoop.pipes.command-file.keep</name>
    <value>true</value>
  </property>
<property>
  <name>hadoop.pipes.executable</name>
  <value>examples/bin/wordcount-py</value>
</property>
<property>
  <name>hadoop.pipes.java.recordreader</name>
  <value>true</value>
</property>
<property>
  <name>hadoop.pipes.java.recordwriter</name>
  <value>true</value>
</property>
</configuration>

zag@manzanillo examples $ hadoop pipes -conf conf/word-python.xml -input in-dir -output our-dir3
09/05/14 10:28:00 WARN mapred.JobClient: No job jar file set.  User classes may not be found. See JobConf(Class) or JobConf#setJar(String).
09/05/14 10:28:00 INFO mapred.FileInputFormat: Total input paths to process : 1
09/05/14 10:28:00 INFO mapred.JobClient: Running job: job_200905141017_0003
09/05/14 10:28:01 INFO mapred.JobClient:  map 0% reduce 0%
09/05/14 10:28:11 INFO mapred.JobClient:  map 50% reduce 0%
09/05/14 10:28:17 INFO mapred.JobClient:  map 100% reduce 0%
09/05/14 10:28:23 INFO mapred.JobClient:  map 100% reduce 16%
09/05/14 10:28:32 INFO mapred.JobClient:  map 100% reduce 100%
09/05/14 10:28:33 INFO mapred.JobClient: Job complete: job_200905141017_0003
09/05/14 10:28:33 INFO mapred.JobClient: Counters: 18
09/05/14 10:28:33 INFO mapred.JobClient:   File Systems
09/05/14 10:28:33 INFO mapred.JobClient:     HDFS bytes read=1464
09/05/14 10:28:33 INFO mapred.JobClient:     HDFS bytes written=883
09/05/14 10:28:33 INFO mapred.JobClient:     Local bytes read=1184
09/05/14 10:28:33 INFO mapred.JobClient:     Local bytes written=2430
09/05/14 10:28:33 INFO mapred.JobClient:   WORDCOUNT
09/05/14 10:28:33 INFO mapred.JobClient:     OUTPUT_WORDS=51
09/05/14 10:28:33 INFO mapred.JobClient:     INPUT_WORDS=74
09/05/14 10:28:33 INFO mapred.JobClient:   Job Counters 
09/05/14 10:28:33 INFO mapred.JobClient:     Launched reduce tasks=1
09/05/14 10:28:33 INFO mapred.JobClient:     Launched map tasks=2
09/05/14 10:28:33 INFO mapred.JobClient:     Data-local map tasks=2
09/05/14 10:28:33 INFO mapred.JobClient:   Map-Reduce Framework
09/05/14 10:28:33 INFO mapred.JobClient:     Reduce input groups=51
09/05/14 10:28:33 INFO mapred.JobClient:     Combine output records=0
09/05/14 10:28:33 INFO mapred.JobClient:     Map input records=32
09/05/14 10:28:33 INFO mapred.JobClient:     Reduce output records=51
09/05/14 10:28:33 INFO mapred.JobClient:     Map output bytes=1030
09/05/14 10:28:33 INFO mapred.JobClient:     Map input bytes=975
09/05/14 10:28:33 INFO mapred.JobClient:     Combine input records=0
09/05/14 10:28:33 INFO mapred.JobClient:     Map output records=74
09/05/14 10:28:33 INFO mapred.JobClient:     Reduce input records=74
