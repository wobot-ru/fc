package ru.wobot

import play.api.libs.json.{JsArray, _}

import scala.collection.mutable

object PageRankPort {
  private final val DAMPENING_FACTOR: Double = 0.85
  private final val EPSILON: Double = 0.0001
  val uniqueIds = new mutable.HashMap[String, String]
  val tweets = new mutable.HashMap[Int, String]
  val tweetid = new mutable.HashMap[Int, String]
  val tweetuserid = new mutable.HashMap[Int, String]
  val inmatr = new mutable.HashMap[String, mutable.Set[String]]
  val outmatr = new mutable.HashMap[String, mutable.Set[String]]

  val tf = new mutable.HashMap[Int, mutable.HashMap[String, Double]]
  val idf = new mutable.HashMap[String, Double]

  def main(args: Array[String]) {
    var index = 0;
    for (line <- scala.io.Source.fromFile("C:\\src\\projects\\Information_Retrieval\\Topic Sensitive PageRank\\mars_tweets_medium.json").getLines()) {
      val field = Json.parse(line)
      val id = (field \ "user" \ "id").get.as[Long].toString
      uniqueIds.put(id, (field \ "user" \ "screen_name").get.as[String])
      for (dt <- (field \ "entities" \ "user_mentions").as[JsArray].value) {
        val dti = (dt \ "id").get.as[Long].toString
        if (dti != id) {
          uniqueIds.put(dti, (dt \ "screen_name").get.as[String])

          if (inmatr.get(dti).isEmpty) inmatr.put(dti, mutable.Set.empty)
          else inmatr.get(dti).get.add(id)

          if (outmatr.get(id).isEmpty) outmatr.put(id, mutable.Set.empty)
          else outmatr.get(id).get.add(dti)

          if (!outmatr.contains(dti)) outmatr.put(dti, mutable.Set.empty)
          if (!inmatr.contains(id)) inmatr.put(id, mutable.Set.empty)
          val text = (field \ "text").get.as[String]
          tweets.put(index, text)
          tweetid.put(index, (field \ "id").get.as[Long].toString)
          tweetuserid.put(index, id)
          processData(text, index)
          index += 1
        }
      }
    }
    println(uniqueIds)
    println(uniqueIds.count(_ => true))
    //println(tweetid)
  }

  def processData(text: String, index: Int) = {
    val set = mutable.Set[String]()
    text.split("\\W+").foreach(word => {
      if (tf.get(index).isEmpty) {
        tf.put(index, new mutable.HashMap[String, Double]())
      }
      tf.get(index).get.put(word, tf.get(index).get.getOrElse(word, 0.0) + 1)
      set.add(word)
    })

    set.map(word => idf.put(word, idf.getOrElse(word, 0.0) + 1))
  }
}
