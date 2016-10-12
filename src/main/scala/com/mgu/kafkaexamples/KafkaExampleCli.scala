package com.mgu.kafkaexamples

import java.util.UUID

import akka.actor.{Actor, ActorSystem, Props}
import com.mgu.kafkaexamples.avro.Message

import scala.io.Source

class KafkaExampleCli(val producer: AvroProducerWorker[Message], val consumer: AvroConsumerWorker[Message]) extends Actor {

  val lines = Source.stdin.getLines

  override def receive: Receive = {
    case line: String => line.split(' ') toList match {
      case "send" :: topic :: xs =>
        val message = Message.newBuilder
          .setMessageId(UUID.randomUUID.toString.substring(0, 7))
          .setText(xs mkString " ")
          .build
        producer.submit(topic, message)
        prompt()
      case "help" :: Nil         => help(); prompt()
      case Nil                   => prompt()
      case "" :: Nil             => prompt()
      case x :: xs               => println(s"Unknown command: ${x}"); prompt()
    }
  }

  private def help() = {
    val helptext =
      """
        |Kafka Scala CLI
        |A Scala-based example of using Kafka
        |
        |The following commands are supported by this CLI:
        |
        |  send <topic> <message>        Sends the message (whitespace allowed) to the given topic.
        |  help                          Displays this helptext.
        |  exit                          Terminates the CLI.
        |
      """.stripMargin
    println(helptext)
  }

  private def prompt() = {
    print("> ")
    if (lines.hasNext) lines.next() match {
      case "exit" =>
        context.system.terminate()
        producer.shutdown()
        consumer.shutdown()
      case line   => self ! line
    }
  }

  override def preStart() = {
    help()
    prompt()
  }
}

object KafkaExampleCli extends App {

  val system = ActorSystem.create("kafka-example-cli")
  val producer = new AvroProducerWorker[Message]
  val producerThread = new Thread(producer)
  val consumer = new AvroConsumerWorker[Message]
  val consumerThread = new Thread(consumer)

  producerThread.start()
  consumerThread.start()

  val cli = system.actorOf(Props(new KafkaExampleCli(producer, consumer)))
}
