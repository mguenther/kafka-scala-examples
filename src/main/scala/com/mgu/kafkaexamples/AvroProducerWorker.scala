package com.mgu.kafkaexamples

import java.util.UUID

import com.mgu.kafkaexamples.AvroProducerWorker._
import com.mgu.kafkaexamples.Settings.ProducerSettings
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.avro.specific.SpecificRecordBase
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.Queue
import scala.reflect.ClassTag

class AvroProducerWorker[T <: SpecificRecordBase : ClassTag](val workerId: String = randomProducerId(),
                                                             val settings: ProducerSettings = ProducerSettings()
                                                               .copy(valueSerializer = "org.apache.kafka.common.serialization.ByteArraySerializer")) extends Runnable {

  @volatile
  private var running = true

  private val workQueue = Queue[UnitOfWork[T]]()

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

  private def nextUnitOfWork(): Option[UnitOfWork[T]] = try {
    Some(workQueue.dequeue())
  } catch {
    case ex: Exception => None
  }

  private def sendImmediately(unitOfWork: UnitOfWork[T]) = {
    serialize(unitOfWork.payload) match {
      case Some(encodedMessage) => {
        val record = new ProducerRecord[String, Array[Byte]](unitOfWork.topic, encodedMessage.asInstanceOf[Array[Byte]])
        underlyingProducer.send(record)
        logger.info(s"[${workerId}] Send message '${unitOfWork.payload}' to topic ${unitOfWork.topic}.")
      }
      case None =>
        logger.warn(s"[${workerId}] Unable to send message ${unitOfWork.payload} to topic ${unitOfWork.topic} due to serialization errors.")
    }
  }

  private def serialize(payload: T): Option[Array[Byte]] = {
    try {
      Some(SpecificAvroCodecs.toBinary[T].apply(payload))
    } catch {
      case ex: Exception =>
        None
    }
  }

  def shutdown() = running = false

  def submit(topic: String, payload: T) = {
    workQueue.enqueue(UnitOfWork(topic, payload))
    logger.info(s"[${workerId}] Accepted message ${payload} for dispatch to topic ${topic}.")
  }
}

object AvroProducerWorker {

  protected val logger: Logger = LoggerFactory getLogger getClass

  case class UnitOfWork[T <: SpecificRecordBase](topic: String, payload: T)

  def randomProducerId(): String = UUID.randomUUID.toString.substring(0, 7)

}
