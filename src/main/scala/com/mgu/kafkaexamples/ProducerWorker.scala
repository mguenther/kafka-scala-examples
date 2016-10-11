package com.mgu.kafkaexamples

import java.util.{Properties, UUID}

import com.mgu.kafkaexamples.ProducerWorker._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.Queue

class ProducerWorker(val workerId: String = UUID.randomUUID.toString.substring(0, 7),
                     val settings: ProducerSettings = ProducerSettings()) extends Runnable {

  @volatile
  private var running = true

  private val workQueue = Queue[UnitOfWork]()

  private lazy val underlyingProducer = new KafkaProducer[String, String](settings.toProperties)

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

  private def nextUnitOfWork(): Option[UnitOfWork] = try {
    Some(workQueue.dequeue())
  } catch {
    case ex: Exception => None
  }

  private def sendImmediately(unitOfWork: UnitOfWork) = {
    serialize(unitOfWork.message) match {
      case Some(jsonString) => {
        val record = new ProducerRecord[String, String](unitOfWork.topic, jsonString)
        underlyingProducer.send(record)
        logger.info(s"Send message '${unitOfWork.message}' to topic ${unitOfWork.topic}.")
      }
      case None =>
        logger.warn(s"Unable to send message ${unitOfWork.message} to topic ${unitOfWork.topic} due to serialization errors.")
    }
  }

  private def serialize(payload: Message): Option[String] = {
    try {
      Some(JsonUtil.toJson(payload))
    } catch {
      case ex: Exception =>
        None
    }
  }

  def shutdown() = running = false

  def submit(topic: String, message: Message) = {
    workQueue.enqueue(UnitOfWork(topic, message))
    logger.info(s"Accepted message ${message} for dispatch to topic ${topic}.")
  }
}

object ProducerWorker {

  protected val logger: Logger = LoggerFactory getLogger getClass

  case class UnitOfWork(topic: String, message: Message)

  case class ProducerSettings(bootstrapServers: List[String] = List("localhost:9092"),
                              keySerializer: String = "org.apache.kafka.common.serialization.StringSerializer",
                              valueSerializer: String = "org.apache.kafka.common.serialization.StringSerializer") {

    def toProperties: Properties = {
      val producerProperties = new Properties()
      producerProperties.put("bootstrap.servers", bootstrapServers mkString ",")
      producerProperties.put("key.serializer", keySerializer)
      producerProperties.put("value.serializer", valueSerializer)
      producerProperties
    }
  }
}
