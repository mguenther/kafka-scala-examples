package com.mgu.kafkaexamples

import java.util.{Properties, UUID}

import akka.actor.{Actor, ActorSystem, Props}

import scala.io.Source

class KafkaExampleCli(val producer: ProducerWorker, val consumer: ConsumerWorker) extends Actor {

  val lines = Source.stdin.getLines

  override def receive: Receive = {
    case line: String => line.split(' ') toList match {
      case "send" :: topic :: xs => producer.submit(topic, Message(text = xs mkString " ")); prompt()
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
  val producer = new ProducerWorker
  val producerThread = new Thread(producer)
  val consumer = new ConsumerWorker
  val consumerThread = new Thread(consumer)

  producerThread.start()
  consumerThread.start()

  val cli = system.actorOf(Props(new KafkaExampleCli(producer, consumer)))
}
