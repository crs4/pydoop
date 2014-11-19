/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Based upon:
 *   https://github.com/AndreSchumacher/avro-parquet-spark-example
 * Modified by CRS4
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
