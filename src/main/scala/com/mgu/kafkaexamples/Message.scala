package com.mgu.kafkaexamples

import java.util.UUID

import play.api.libs.json.{Reads, Json, JsValue, Writes}
import play.api.libs.functional.syntax._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.data.validation.ValidationError
import play.api.libs.json.Reads._

case class Message(messageId: String = UUID.randomUUID().toString.substring(0, 7), text: String)

object Message {

  /*implicit val messageJsonWrite = new Writes[Message] {
    def writes(message: Message): JsValue = {
      Json.obj(
        "messageId" -> message.messageId,
        "text" -> message.text
      )
    }
  }*/

  /*implicit val messageJsonReads = new Reads[Message] {
    (
      (__ \ 'messageId).read[String] ~
      (__ \ 'text).read[String]
      )(Message.apply _)
  }*/

  /*implicit val reads: Reads[Message] = (
    (__ \ "messageId").read[String] and
      (__ \ "text").read[String]
    )*/

  implicit val format: Format[Message] = Json.format[Message]
}


/*
object MyType {

  implicit val myTypeJsonWrite = new Writes[MyType] {
    def writes(type: MyType): JsValue = {
      Json.obj(
        "type" -> MyType.type
      )
    }
  }

  implicit val myTypeJsonRead = (
    (__ \ 'type).read[String]
  )(MyType.apply _)
}
 */