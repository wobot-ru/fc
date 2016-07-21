package ru.wobot

import play.api.libs.json.JsValue

class Profile(val id: Long,
              val name: String,
              val friends: Seq[Long],
              val json: JsValue) {
  override def toString: String = s"$id $name $friends $json"
}
