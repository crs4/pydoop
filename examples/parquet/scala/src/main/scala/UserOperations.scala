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
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import parquet.avro.AvroParquetWriter
import parquet.hadoop.ParquetInputFormat

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, IndexedRecord}
import org.apache.avro.specific.{SpecificRecord, SpecificDatumWriter}

import scala.reflect.ClassTag // ??

// our own class generated from user.avdl by Avro tools
import pydoop.avrotest.avro.{Message, User}


object UserOperations {
  def writeAvroFile[T <: SpecificRecord](
    file: File,
    factoryMethod: Int => T,
    count: Int): Unit = {
    val prototype = factoryMethod(0)
    val datumWriter = new SpecificDatumWriter[T](
      prototype.getClass.asInstanceOf[java.lang.Class[T]])
    val dataFileWriter = new DataFileWriter[T](datumWriter)
    
    dataFileWriter.create(prototype.getSchema, file)
    for (i <- 1 to count) {
      dataFileWriter.append(factoryMethod(i))
    }
    dataFileWriter.close()
  }
  def writeParquetFile[T <: SpecificRecord](
    file: Path,
    factoryMethod: Int => T,
    count: Int): Unit = {
      val prototype = factoryMethod(0)
      val schema = prototype.getSchema
      val parquetWriter = new AvroParquetWriter[IndexedRecord](file, schema)

      for (i <- 1 to count) {
        parquetWriter.write(factoryMethod(i))
      }
      parquetWriter.close()
  }
  /**
   * Creates a User Avro object.
   *
   * @param id The ID of the user to generate
   * @return An Avro object that represents the user
   */
  def createUser(id: Int): User = {
    val builder = User.newBuilder()
      .setName(s"User$id")
      .setAge(id / 10)
    if (id >= 5) {
      builder
        .setFavoriteColor("blue")
        .build()
    } else {
      builder
        .setFavoriteColor("red")
        .build()
    }
  }

  /**
   * Creates a Message Avro object.
   *
   * @param id The ID of the message to generate
   * @return an Avro object that represents the mssage
   */
  def createMessage(maxUserId: Int)(id: Int): Message = {
    val sender = Random.nextInt(maxUserId)
    var recipient = Random.nextInt(maxUserId)
    while (recipient == sender) recipient = Random.nextInt(maxUserId)

    Message.newBuilder()
      .setID(id)
      .setSender(s"User$sender")
      .setRecipient(s"User$recipient")
      .setContent(s"Hey there, User$recipient, this is me, User$sender")
      .build()
  }

}

