Check if ldd is loading the right jvm.

zag@manzanillo libhdfs $ echo $CLASSPATH 
/opt/hadoop-0.19.1/lib/xmlenc-0.52.jar:/opt/hadoop-0.19.1/lib/slf4j-log4j12-1.4.3.jar:/opt/hadoop-0.19.1/lib/slf4j-api-1.4.3.jar:/opt/hadoop-0.19.1/lib/servlet-api.jar:/opt/hadoop-0.19.1/lib/oro-2.0.8.jar:/opt/hadoop-0.19.1/lib/log4j-1.2.15.jar:/opt/hadoop-0.19.1/lib/kfs-0.2.0.jar:/opt/hadoop-0.19.1/lib/junit-3.8.1.jar:/opt/hadoop-0.19.1/lib/jetty-5.1.4.jar:/opt/hadoop-0.19.1/lib/jets3t-0.6.1.jar:/opt/hadoop-0.19.1/lib/hsqldb-1.8.0.10.jar:/opt/hadoop-0.19.1/lib/commons-net-1.4.1.jar:/opt/hadoop-0.19.1/lib/commons-logging-api-1.0.4.jar:/opt/hadoop-0.19.1/lib/commons-logging-1.0.4.jar:/opt/hadoop-0.19.1/lib/commons-httpclient-3.0.1.jar:/opt/hadoop-0.19.1/lib/commons-codec-1.3.jar:/opt/hadoop-0.19.1/lib/commons-cli-2.0-SNAPSHOT.jar:/opt/hadoop-0.19.1/hadoop-0.19.1-core.jar:/opt/hadoop-0.19.1/hadoop-0.19.1-tools.jar:/opt/hadoop-0.19.1/conf



zag@manzanillo libhdfs $ ldd hdfs_test
	linux-vdso.so.1 =>  (0x00007fffe85fd000)
	libhdfs.so => ./libhdfs.so (0x00007f0ae0136000)
	libc.so.6 => /lib/libc.so.6 (0x00007f0adfdee000)
	libjvm.so => /opt/blackdown-jdk-1.4.2.03/jre/lib/amd64/server/libjvm.so (0x00007f0adf64a000)
	/lib64/ld-linux-x86-64.so.2 (0x00007f0ae0340000)
	libnsl.so.1 => /lib/libnsl.so.1 (0x00007f0adf433000)
	libm.so.6 => /lib/libm.so.6 (0x00007f0adf1b2000)
	libdl.so.2 => /lib/libdl.so.2 (0x00007f0adefae000)
	libpthread.so.0 => /lib/libpthread.so.0 (0x00007f0aded93000)
zag@manzanillo libhdfs $ echo $LD_LIBRARY_PATH 
.:/home/zag/.mozilla/lib:/home/zag/.mozilla/lib
zag@manzanillo libhdfs $ export  LD_LIBRARY_PATH=/opt/sun-jdk-1.6.0.13/jre/lib/amd64/server:$LD_LIBRARY_PATH 
zag@manzanillo libhdfs $ ./hdfs_test 
09/05/15 22:01:49 INFO ipc.Client: Retrying connect to server: localhost/127.0.0.1:9000. Already tried 0 time(s).
09/05/15 22:01:50 INFO ipc.Client: Retrying connect to server: localhost/127.0.0.1:9000. Already tried 1 time(s).
^Czag@manzanillo libhdfs $ ./hdfs_read 
Usage: hdfs_read <filename> <filesize> <buffersize>


