name := "kafka-scala-examples"

val kafkaVersion = "0.9.0.1"
val akkaVersion = "2.4.2"

version := "0.1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.1.6",
  "com.typesafe.play" %% "play-json" % "2.3.4",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.3",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.3",
  "org.scalatest" %% "scalatest" % "2.2.6" % Test)

