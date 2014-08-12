/*
 * Code copied almost verbatim from
 * https://github.com/AndreSchumacher/avro-parquet-spark-example
 */

package pydoop.avro

import java.io.File
import org.apache.hadoop.fs.Path

import pydoop.avrotest.avro.{Message, User}

import pydoop.avro.UserOperations._

object WriteParquet {
  def main(args: Array[String]) {
    // just for kicks, we first write avro
    val avroFiles = (new File("foo-users.avro"), 
                     new File("foo-messages.avro"))
    writeAvroFile(avroFiles._1, createUser, 100)
    writeAvroFile(avroFiles._2, createMessage(100)_, 1000)
    writeParquetFile(new Path("foo-users.parquet"), createUser, 10)
    writeParquetFile(new Path("foo-messages.parquet"), createMessage(100)_, 100)
  }
}
