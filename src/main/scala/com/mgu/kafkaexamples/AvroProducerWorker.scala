package com.mgu.kafkaexamples

import java.util.UUID

import com.mgu.kafkaexamples.AvroProducerWorker._
import com.mgu.kafkaexamples.Settings.ProducerSettings
import com.mgu.kafkaexamples.avro.Message
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.Queue

class AvroProducerWorker(val workerId: String = UUID.randomUUID.toString.substring(0, 7),
                         val settings: ProducerSettings = ProducerSettings().copy(valueSerializer = "org.apache.kafka.common.serialization.ByteArraySerializer")) extends Runnable {

  @volatile
  private var running = true

  private val workQueue = Queue[UnitOfWork]()

  private lazy val underlyingProducer = new KafkaProducer[String, Array[Byte]](settings.toProperties)

  override def run() = {
    logger.info(s"${workerId}] Initialized underlying Kafka producer.")
    while (running) {
      nextUnitOfWork() match {
      case Some(unitOfWork) => sendImmediately(unitOfWork)
      case None => Thread.sleep(1000)
    }
  }
  logger.info(s"[${workerId}] Closing underlying Kafka producer.")
  underlyingProducer.close()
  logger.info(s"[${workerId}] Underlying Kafka producer has been closed.")
  }

  private def nextUnitOfWork(): Option[UnitOfWork] = try {
    Some(workQueue.dequeue())
  } catch {
    case ex: Exception => None
  }

  private def sendImmediately(unitOfWork: UnitOfWork) = {
    serialize(unitOfWork.message) match {
      case Some(encodedMessage) => {
        val record = new ProducerRecord[String, Array[Byte]](unitOfWork.topic, encodedMessage)
        underlyingProducer.send(record)
        logger.info(s"[${workerId}] Send message '${unitOfWork.message}' to topic ${unitOfWork.topic}.")
      }
      case None =>
        logger.warn(s"[${workerId}] Unable to send message ${unitOfWork.message} to topic ${unitOfWork.topic} due to serialization errors.")
    }
  }

  private def serialize(payload: Message): Option[Array[Byte]] = {
    try {
      Some(SpecificAvroCodecs.toBinary[Message].apply(payload))
    } catch {
      case ex: Exception =>
        None
    }
  }

  def shutdown() = running = false

  def submit(topic: String, message: Message) = {
    workQueue.enqueue(UnitOfWork(topic, message))
    logger.info(s"[${workerId}] Accepted message ${message} for dispatch to topic ${topic}.")
  }
}

object AvroProducerWorker {

  protected val logger: Logger = LoggerFactory getLogger getClass

  case class UnitOfWork(topic: String, message: Message)
}
