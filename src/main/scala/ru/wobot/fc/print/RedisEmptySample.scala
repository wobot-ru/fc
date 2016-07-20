package ru.wobot.fc.print

import java.util.concurrent.TimeUnit

import com.redis.RedisClientPool
import com.redis.serialization.Parse.Implicits._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object RedisEmptySample {
  def main(args: Array[String]) {
    val clients = new RedisClientPool("localhost", 6379)
    clients.withClient(x => {
      x.set("vk://id1",1)
//      val eventualMaybeInt: Future[Option[Int]] = Future {
//        x.get[Int]("vk://id12")
//      }
//      val i = Await.result(eventualMaybeInt, Duration.create(1, TimeUnit.SECONDS))
//      println(i.get)
    })

    clients.close
  }

}