package com.mgu.kafkaexamples

import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.avro.specific.SpecificRecordBase
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, Serializer, Deserializer}
import org.slf4j.{LoggerFactory, Logger}

import scala.reflect.ClassTag

class AvroCodec[T <: SpecificRecordBase : ClassTag](override val manifest: Manifest[T]) extends Codec[T, Array[Byte]] {

  import AvroCodec._

  override def encode(input: T): Option[Array[Byte]] = try {
    Some(SpecificAvroCodecs.toBinary[T].apply(input))
  } catch {
    case ex: Exception =>
      logger.warn(s"Unable to encode input ${input}.")
      None
  }

  override def compatibleKafkaSerializer: Class[_ <: Serializer[Array[Byte]]] = classOf[ByteArraySerializer]

  override def compatibleKafkaDeserializer: Class[_ <: Deserializer[Array[Byte]]] = classOf[ByteArrayDeserializer]

  override def decode(output: Array[Byte])(implicit tag: Manifest[T]): Option[T] = try {
    Some(SpecificAvroCodecs.toBinary[T].invert(output).get)
  } catch {
    case ex: Exception =>
      logger.warn(s"Unable to decode ${output} to instance of class ${manifest.getClass}.")
      None
  }
}

object AvroCodec {

  protected val logger: Logger = LoggerFactory getLogger getClass
}
