package ru.wobot

import play.api.libs.json.JsValue

class Profile(val id: Long,
              val name: String,
              val friends: Seq[Long],
              val json: JsValue) {
  override def toString: String = s"$name http://vk.com/id$id $friends $json"
}
