package com.mgu.kafkaexamples

import java.util.UUID

import com.mgu.kafkaexamples.ProducerWorker._
import com.mgu.kafkaexamples.Settings.ProducerSettings
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.Queue
import scala.reflect.ClassTag

class ProducerWorker[I: ClassTag, O: ClassTag](val workerId: String = UUID.randomUUID.toString.substring(0, 7),
                                               val settings: ProducerSettings[I, O]) extends Runnable {

  @volatile
  private var running = true

  private val workQueue = Queue[UnitOfWork[I]]()

  private val encoder: Encoder[I, O] = settings.encoder

  private lazy val underlyingProducer = new KafkaProducer[String, O](settings.toProperties)

  override def run() = {
    logger.info(s"[${workerId}] Initialized underlying Kafka producer.")
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

  private def nextUnitOfWork(): Option[UnitOfWork[I]] = try {
    Some(workQueue.dequeue())
  } catch {
    case ex: Exception => None
  }

  private def sendImmediately(unitOfWork: UnitOfWork[I]) = {
    encoder.encode(unitOfWork.payload) match {
      case Some(jsonString) => {
        val record = new ProducerRecord[String, O](unitOfWork.topic, jsonString)
        underlyingProducer.send(record)
        logger.info(s"[${workerId}] Send message '${unitOfWork.payload}' to topic ${unitOfWork.topic}.")
      }
      case None =>
        logger.warn(s"[${workerId}] Unable to send message ${unitOfWork.payload} to topic ${unitOfWork.topic} due to serialization errors.")
    }
  }

  def shutdown() = running = false

  def submit(topic: String, payload: I) = {
    workQueue.enqueue(UnitOfWork(topic, payload))
    logger.info(s"[${workerId}] Accepted message ${payload} for dispatch to topic ${topic}.")
  }
}

object ProducerWorker {

  protected val logger: Logger = LoggerFactory getLogger getClass

  case class UnitOfWork[I](topic: String, payload: I)
}
