package com.mgu.kafkaexamples

import java.util.{Collections, UUID}

import akka.actor.{Actor, ActorSystem, Props}
import com.mgu.kafkaexamples.KafkaExampleCli.{EmployeeV1_0, EmployeeV2_0}
import com.mgu.kafkaexamples.Messages.Message
import com.mgu.kafkaexamples.Settings.{ProducerSettings, ConsumerSettings}
//import com.mgu.kafkaexamples.avro.Message

import scala.io.Source

class KafkaExampleCli(val producer: ProducerWorker[Message, String], val consumer: ConsumerWorker[Message, String]
                      //val employeeProducer: AvroProducerWorker[EmployeeV1_0],
                      /*val employeeConsumer: AvroConsumerWorker[EmployeeV2_0]*/) extends Actor {

  val lines = Source.stdin.getLines

  override def receive: Receive = {
    case line: String => line.split(' ') toList match {
      /*case "employee" :: Nil =>
        val employee: EmployeeV1_0 = new EmployeeV1_0("Max Mustermann", 2, Collections.singletonList("max.mustermann@test.de"), null)
        //val employee: EmployeeV2_0 = new EmployeeV2_0("Max Mustermann", 2, "male", Collections.singletonList("max.mustermann@test.de"))
        employeeProducer.submit("employee", employee)
        prompt()*/
      case "send" :: topic :: xs =>
        /*val message = Message.newBuilder
          .setMessageId(UUID.randomUUID.toString.substring(0, 7))
          .setText(xs mkString " ")
          .build*/
        val message = Message(text = xs mkString " ")
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

  type EmployeeV1_0 = com.mgu.kafkaexamples.avro.v1_0.Employee
  type EmployeeV2_0 = com.mgu.kafkaexamples.avro.v2_0.Employee

  val codec = new JsonCodec[Message](manifest[Message])

  val producerSettings = ProducerSettings(encoder = codec)
  val consumerSettings = ConsumerSettings(decoder = codec)

  val system = ActorSystem.create("kafka-example-cli")
  val producer = new ProducerWorker[Message, String](settings = producerSettings)
  val producerThread = new Thread(producer)
  val consumer = new ConsumerWorker[Message, String](settings = consumerSettings)
  val consumerThread = new Thread(consumer)
  /*val employeeProducer = new AvroProducerWorker[EmployeeV1_0]
  val employeeProducerThread = new Thread(employeeProducer)
  val employeeConsumer = new AvroConsumerWorker[EmployeeV2_0](settings = ConsumerSettings().copy(topics = List("employee"), valueDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer"))
  val employeeConsumerThread = new Thread(employeeConsumer)*/

  producerThread.start()
  //employeeProducerThread.start()
  consumerThread.start()
  //employeeConsumerThread.start()

  val cli = system.actorOf(Props(new KafkaExampleCli(producer, consumer/*, employeeProducer, employeeConsumer*/)))
}
