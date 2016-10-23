package com.mgu.kafkaexamples

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.slf4j.{LoggerFactory, Logger}

import scala.reflect.ClassTag

class JsonCodec[T : ClassTag](override val manifest: Manifest[T]) extends Codec[T, String] {

  import JsonCodec._

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  override def encode(input: T): Option[String] =
    try {
      Some(mapper.writeValueAsString(input))
    } catch {
      case ex: Exception =>
        logger.warn(s"Unable to encode input ${input} to JSON-based String.")
        None
    }

  override def compatibleKafkaSerializer: Class[_ <: Serializer[String]] = classOf[StringSerializer]

  override def compatibleKafkaDeserializer: Class[_ <: Deserializer[String]] = classOf[StringDeserializer]

  override def decode(output: String)(implicit m: Manifest[T]): Option[T] =
    try {
      Some(mapper.readValue[T](output))
    } catch {
      case ex: Exception =>
        logger.warn(s"Unable to decode JSON ${output} to instance of class ${manifest.getClass}.")
        None
    }
}

object JsonCodec {

  protected val logger: Logger = LoggerFactory getLogger getClass
}