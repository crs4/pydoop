name := "ParquetMR"

version := "0.1"

publishMavenStyle := true

crossPaths := false

autoScalaLibrary := false

seq( sbtavro.SbtAvro.avroSettings : _*)

(version in avroConfig) := "1.7.7"

val parquetVersion = "1.5.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client"  % "2.4.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs"  % "2.4.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-common"  % "2.4.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-client"  % "2.4.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-common"  % "2.4.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-server-common"  % "2.4.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core"  % "2.4.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-app"  % "2.4.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-common"  % "2.4.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-shuffle"  % "2.4.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient"  % "2.4.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-annotations"  % "2.4.1" % "provided"


libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.7"


libraryDependencies += "com.twitter" % "parquet-common" % parquetVersion
libraryDependencies += "com.twitter" % "parquet-column" % parquetVersion
libraryDependencies += "com.twitter" % "parquet-hadoop" % parquetVersion
libraryDependencies += "com.twitter" % "parquet-avro" % parquetVersion
