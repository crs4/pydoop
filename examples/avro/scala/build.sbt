name := "ParquetWriter"

version := "0.1"

scalaVersion := "2.11.1"

seq( sbtavro.SbtAvro.avroSettings : _*)

(version in avroConfig) := "1.7.6"

libraryDependencies += "org.apache.hadoop" % "hadoop-client"  % "2.4.1"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.7"
