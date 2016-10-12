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
  "org.apache.commons" % "commons-exec" % "1.3",
  "com.google.guava" % "guava" % "19.0",
  "org.apache.avro" % "avro" % "1.6.3",
  "com.twitter" %% "bijection-avro" % "0.9.2",
  "org.scalatest" %% "scalatest" % "2.2.6" % Test)

Seq( sbtavro.SbtAvro.avroSettings : _*)

//E.g. put the source where IntelliJ can see it 'src/main/java' instead of 'targe/scr_managed'.
javaSource in sbtavro.SbtAvro.avroConfig <<= (sourceDirectory in Compile)(_ / "generated")

(stringType in avroConfig) := "String"
