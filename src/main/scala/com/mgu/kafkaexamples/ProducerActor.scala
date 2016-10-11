package com.mgu.kafkaexamples

import java.util.Properties

import akka.actor.{Actor, PoisonPill}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class ProducerActor[T](val id: String, val props: Properties) extends Actor {

  import ProducerActor._

  private val underlyingProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  override def receive: Receive = {
    case Log(topic, payload: T) =>
      val json = JsonUtil.toJson(payload)
      val record = new ProducerRecord[String, String](topic, json)
      underlyingProducer.send(record)
    case KeyedLog(topic, key, payload: T) =>
      val json = JsonUtil.toJson(payload)
      val record = new ProducerRecord[String, String](topic, key, json)
      underlyingProducer.send(record)
    case PoisonPill => underlyingProducer.close()
  }
}

object ProducerActor {
  case class Log[T](topic: String, payload: T)
  case class KeyedLog[T](topic: String, key: String, payload: T)
}
