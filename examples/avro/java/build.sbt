name := "AvroMR"

version := "0.1"

publishMavenStyle := true

crossPaths := false

autoScalaLibrary := false

seq( sbtavro.SbtAvro.avroSettings : _*)

(version in avroConfig) := "1.7.7"

libraryDependencies += "org.apache.avro" % "avro" % "1.7.7" 

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.1"

javacOptions += "-Xlint:unchecked"

javacOptions += "-Xlint:deprecation"

