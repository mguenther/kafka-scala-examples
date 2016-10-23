package com.mgu.kafkaexamples

import org.apache.kafka.common.serialization.{Deserializer, Serializer}

/**
  * An `Encoder` knows how to transform a given input of type `I` to an output
  * of type `O`. By providing an implementation for [[Encoder#compatibleKafkaSerializer]]
  * it is ensured that the underlying Kafka serialization mechanism is compatible
  * with the output type `O`.
  *
  * An `Encoder` must not make any guarantees wrt. thread-safety. Client classes that
  * use an `Encoder` have to provide the proper concurrency mechanisms if need be.
  *
  * @tparam I
  *   represents the input type
  * @tparam O
  *   represents the output type
  */
trait Encoder[I, O] {

  /**
    * Transforms the given input of type `I` to an output of type `O`.
    *
    * @param input
    *   instance of type `I`
    * @return
    *   `Some[O]` if encoding the input of type `I` has been successful,
    *   `None` otherwise.
    */
  def encode(input: I): Option[O]

  /**
    * @return
    *   The [[Serializer]] that is compatible with the output type `O`
    *   of this `Encoder`.
    */
  def compatibleKafkaSerializer: Class[_ <: Serializer[O]]
}

/**
  * A [[Decoder]] knows how to transform an output of type `O` back to an input
  * of type `I`. Hence, it is the inverse operation to [[Encoder#encode]] if both
  * input and output types match. By providing an implementation for
  * [[Decoder#compatibleKafkaDeserializer]] it is ensured that the underyling Kafka
  * serialization mechanism is compatible with the output type `O`.
  *
  * A `Decoder` must not make any guarantees wrt. thread-safety. Client classes that
  * use a `Decoder` have to provide the proper concurrency mechanism if need be.
  *
  * @tparam I
  *   represents the input type
  * @tparam O
  *   represents the output type
  */
trait Decoder[I, O] {

  def manifest: Manifest[I] = ???

  /**
    * Transforms the given output of type `O` to an instance of the input
    * type `I`.
    *
    * @param output
    *   `Some[I]` if decoding the input of type `O` has been successful.
    *   `None` otherwise.
    * @return
    */
  def decode(output: O)(implicit tag: Manifest[I]): Option[I]

  /**
    * @return
    *   The [[Deserializer]] that is compatible with the output type of `O`
    *   of this `Decoder`.
    */
  def compatibleKafkaDeserializer: Class[_ <: Deserializer[O]]
}

/**
  * A `Codec` represents the combination of an [[Encoder]] with a compatible
  * [[Decoder]] such that [[Encoder#encode]] and [[Decoder#decode]] form a bijection
  * between input type `I` and output type `O`. It is expected that a round-trip
  * conversion between those types does not result in data loss.
  *
  * @tparam I
  *   represents the input type
  * @tparam O
  *   represents the output type
  */
trait Codec[I, O] extends Encoder[I, O] with Decoder[I, O] {
}
