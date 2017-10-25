name := "ParquetMR"

version := "0.1"

publishMavenStyle := true

crossPaths := false

autoScalaLibrary := false

(version in AvroConfig) := "1.7.6"

val parquetRoot = "org.apache.parquet"

// Parquet versions earlier than 1.6.0 have problems with object reuse:
//   https://issues.apache.org/jira/browse/PARQUET-62
val parquetVersion = "1.8.1"

val hadoopVersion = "2.7.4"

val avroVersion = "1.7.6"

libraryDependencies ++= Seq(
	"org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
	"org.slf4j" % "slf4j-log4j12" % "1.7.7",
	parquetRoot % "parquet-common" % parquetVersion,
	parquetRoot % "parquet-column" % parquetVersion,
	parquetRoot % "parquet-hadoop" % parquetVersion,
	parquetRoot % "parquet-avro" % parquetVersion,
	"org.apache.avro" % "avro-mapred" % avroVersion
)
