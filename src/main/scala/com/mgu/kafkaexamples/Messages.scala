package com.mgu.kafkaexamples

import java.util.UUID

import play.api.libs.json.{Json, _}

object Messages {

  implicit val format: Format[Message] = Json.format[Message]

  case class Message(messageId: String = UUID.randomUUID().toString.substring(0, 7), text: String)
}