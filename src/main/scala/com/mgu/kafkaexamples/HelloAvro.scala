package com.mgu.kafkaexamples

import java.io.{ByteArrayOutputStream, File}

import org.apache.avro.SchemaBuilder
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecordBuilder, GenericRecord}
import org.apache.avro.io.EncoderFactory

object HelloAvro {
  def main(args: Array[String]) {
    // Build a schema
    val schema = SchemaBuilder
      .record("Message")
      .fields
      .name("messageId").`type`().stringType().noDefault()
      .name("text").`type`().stringType().noDefault()
      .endRecord

    // Build an object conforming to the schema
    val user1 = new GenericRecordBuilder(schema)
      .set("messageId", "f9ae42fc")
      .set("text", "Hello!")
      .build

    // JSON encoding of the object (a single record)
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val baos = new ByteArrayOutputStream
    val jsonEncoder = EncoderFactory.get.jsonEncoder(schema, baos)
    writer.write(user1, jsonEncoder)
    jsonEncoder.flush
    println("JSON encoded record: " + baos)

    // binary encoding of the object (a single record)
    baos.reset
    val binaryEncoder = EncoderFactory.get.binaryEncoder(baos, null)
    writer.write(user1, binaryEncoder)
    binaryEncoder.flush
    println("Binary encoded record: " + baos.toByteArray)

    // Build another object conforming to the schema
    /*val user2 = new GenericRecordBuilder(schema)
      .set("name", "Sam")
      .set("ID", 2)
      .build

    // Write both records to an Avro object container file

    //file.deleteOnExit*/
    val file = new File("message-container.avro")
    val dataFileWriter = new DataFileWriter[GenericRecord](writer)
    dataFileWriter.create(schema, file)
    dataFileWriter.append(user1)
    //dataFileWriter.append(user2)
    dataFileWriter.close

    // Read the records back from the file
    val datumReader = new GenericDatumReader[GenericRecord](schema)
    val dataFileReader = new DataFileReader[GenericRecord](file, datumReader)
    var user: GenericRecord = null;
    while (dataFileReader.hasNext) {
      user = dataFileReader.next(user)
      println("Read user from Avro file: " + user)
    }
  }
}
