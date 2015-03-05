name := "ParquetMR"

version := "0.1"

publishMavenStyle := true

crossPaths := false

autoScalaLibrary := false

seq( sbtavro.SbtAvro.avroSettings : _*)

(version in avroConfig) := "1.7.4"

val parquetVersion = "1.5.0"

val hadoopVersion = "2.6.0"

val avroVersion = "1.7.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided"


libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.7"


libraryDependencies += "com.twitter" % "parquet-common" % parquetVersion

libraryDependencies += "com.twitter" % "parquet-column" % parquetVersion

libraryDependencies += "com.twitter" % "parquet-hadoop" % parquetVersion

libraryDependencies += "com.twitter" % "parquet-avro" % parquetVersion


libraryDependencies += "org.apache.avro" % "avro-mapred" % avroVersion
