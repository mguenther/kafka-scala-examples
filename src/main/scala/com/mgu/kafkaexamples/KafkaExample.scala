package com.mgu.kafkaexamples

import java.util.Properties

import akka.actor.{ActorSystem, Props}
import com.mgu.kafkaexamples.ProducerActor.Log

object KafkaExample extends App {

  val system = ActorSystem.create("test")

  val  producerProperties = new Properties()
  producerProperties.put("bootstrap.servers", "localhost:9092")
  producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producerActor = system.actorOf(Props(new ProducerActor[String]("p-a-1", producerProperties)))

  val consumer = new ConsumerWorker
  val consumerThread = new Thread(consumer)
  consumerThread.start()

  Thread.sleep(1000)

  producerActor ! Log("test", Message(text = "Dies ist ein Test."))
}
