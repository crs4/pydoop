name := "ParquetMR"

version := "0.1"

publishMavenStyle := true

crossPaths := false

autoScalaLibrary := false

seq( sbtavro.SbtAvro.avroSettings : _*)

(version in avroConfig) := "1.7.4"

val parquetRoot = "com.twitter"

// Parquet versions earlier than 1.6.0 have problems with object reuse:
//   https://issues.apache.org/jira/browse/PARQUET-62
val parquetVersion = "1.6.0"

val hadoopVersion = "2.6.0"

val avroVersion = "1.7.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"


libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.7"

libraryDependencies += parquetRoot % "parquet-common" % parquetVersion

libraryDependencies += parquetRoot % "parquet-column" % parquetVersion

libraryDependencies += parquetRoot % "parquet-hadoop" % parquetVersion

libraryDependencies += parquetRoot % "parquet-avro" % parquetVersion


libraryDependencies += "org.apache.avro" % "avro-mapred" % avroVersion
