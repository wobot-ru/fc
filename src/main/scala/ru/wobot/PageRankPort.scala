package ru.wobot

import play.api.libs.json.{JsArray, _}

import scala.collection.mutable

object PageRankPort {
  private final val DAMPENING_FACTOR: Double = 0.85
  private final val EPSILON: Double = 0.0001
  val uniqueIds = new mutable.HashMap[String, String]
  val tweets = new mutable.HashMap[Long, String]
  val tweetid = new mutable.HashMap[Long, String]
  val tweetuserid = new mutable.HashMap[Long, String]
  val tweetuserscore = new mutable.HashMap[String, Double]
  val inmatr = new mutable.HashMap[String, mutable.Set[String]]
  val outmatr = new mutable.HashMap[String, mutable.Set[String]]

  val tf = new mutable.HashMap[Long, mutable.HashMap[String, Double]]
  val tfidf = new mutable.HashMap[Long, mutable.HashMap[String, Double]]
  val idf = new mutable.HashMap[String, Double]

  def main(args: Array[String]) {
    var index = 0
    for (line <- scala.io.Source.fromFile("C:\\src\\projects\\Information_Retrieval\\Topic Sensitive PageRank\\mars_tweets_medium_big.json").getLines()) {
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
        }
      }

      val text = (field \ "text").get.as[String]
      tweets.put(index, text)
      tweetid.put(index, (field \ "id").get.as[Long].toString)
      tweetuserid.put(index, id)
      processData(text, index)
      index += 1
    }
    val ranklst = calcPgRank(uniqueIds, inmatr)
    index = 0
    for ((k, v) <- tweetid) {
      val userId: String = tweetuserid.get(index).get
      val userRank = ranklst.get(userId).get
      tweetuserscore.put(v, userRank)
      index += 1
    }

    for ((k, v) <- idf) {
      val n = index / v
      idf.put(k, Math.log(n) / Math.log(2))
    }

    for ((k, v) <- tf) {
      var sum: Double = 0D
      for ((m, n) <- v) {
        if (tfidf.get(k).isEmpty)
          tfidf(k) = new mutable.HashMap[String, Double]()
        val l = Math.log(tf(k)(m)) / Math.log(2)
        tfidf(k)(m) = (1 + l) * idf(m)
        sum += (tfidf(k)(m)) * (tfidf(k)(m))
      }
      val norm = Math.sqrt(sum)
      for ((i, j) <- v) {
        tfidf(k)(i) = tfidf(k)(i) / norm
      }
    }

    inmatr.take(100).foreach(x => println(s"${x._1}, ${x._2.mkString(",")}"))
    println(uniqueIds.count(_ => true))
    println("========================")
    println(ranklst)
    println("========================")
    var ok = true
    while (ok) {
      print("Enter search query: ")
      val ln = scala.io.StdIn.readLine()
      processQuery(ln)
      ok = ln != null && ln.nonEmpty
    }

  }

  def processData(text: String, index: Long) = {
    val set = mutable.Set[String]()
    text.toLowerCase.split("\\W+").foreach(word => {
      if (tf.get(index).isEmpty) {
        tf.put(index, new mutable.HashMap[String, Double]())
      }
      tf.get(index).get.put(word, tf.get(index).get.getOrElse(word, 0.0) + 1)
      set.add(word)
    })

    set.map(word => idf.put(word, idf.getOrElse(word, 0.0) + 1))
  }

  def calcPgRank(uniqueIds: mutable.HashMap[String, String], inmatr: mutable.HashMap[String, mutable.Set[String]]) = {
    val alpha = 0.1
    var ranklist = new mutable.HashMap[String, Double]
    val currlist = new mutable.HashMap[String, Double]
    val inmatrLen = inmatr.size
    val outmatrLen = outmatr.size
    uniqueIds.foreach(_ match {
      case (node, name) => {
        if (inmatr.contains(node)) {
          ranklist.put(node, 1 / inmatrLen)
        }
      }
    })

    var done = false
    while (!done) {
      uniqueIds.foreach(_ match {
        case (node, name) => {
          currlist.put(node, 0)
          if (inmatr.contains(node)) {
            currlist.put(node, alpha / inmatrLen)
            inmatr.get(node).get.foreach(innode => {
              if (currlist.get(innode).isEmpty) currlist.put(innode, 0.0)
              var r = currlist.get(innode).get
              r += (1 - alpha) * (ranklist.get(innode).get / outmatr.get(innode).get.size)
              currlist.put(innode, r)
              if (Math.abs(currlist.get(node).get - ranklist.get(node).get) > 0.0001) done = true
            })
          }
        }
      })
      ranklist = currlist.clone()
    }

    currlist
  }


  def processQuery(str: String): Unit = {
    val qtf = new mutable.HashMap[String, Double]
    val qtfidf = new mutable.HashMap[String, Double]
    str.toLowerCase.split("\\W+").foreach(word => {
      if (qtf.get(word).isEmpty) qtf(word) = 0
      qtf(word) = qtf(word) + 1
    })
    var sum = 0D
    for ((k, v) <- qtf) {
      qtf(k) = (1 + Math.log(qtf(k)) / Math.log(2))
      qtfidf(k) = qtf(k) * idf(k)
      sum += qtfidf(k) * qtfidf(k)
      if (qtfidf(k) == 0) return
    }
    val norm = Math.sqrt(sum)
    for ((i, j) <- qtfidf)
      qtfidf(i) = qtfidf(i) / norm

    val pgrank = buildRank(qtfidf)
    val topdict = pgrank.toList.sortBy(x => x._2).reverse.take(50)
    val finaldict: List[(Long, Double, Double)] = topdict.map((x: (Long, Double)) => (x._1, x._2, tweetuserscore(tweetid(x._1)))).sortBy(x => (x._2, x._3)).reverse.take(50)
    var rank = 1
    for ((key, score, userscore) <- finaldict) {
      println(s"rank:  $rank tweetid:  ${tweetid(key)}  tweet:  ${tweets(key)}")
      rank += 1
    }
  }

  def buildRank(qtfidf: mutable.HashMap[String, Double]): mutable.HashMap[Long, Double] = {
    val rank = new mutable.HashMap[Long, Double]
    for ((k: Long, v) <- tfidf) {
      for ((i, j) <- qtfidf) {
        if (qtfidf.get(i).isEmpty) qtfidf.put(i, 0)
        if (v.get(i).isEmpty) v.put(i, 0)
        rank.put(k, rank.getOrElse(k, 0D) + qtfidf(i) * v(i))
      }
    }
    rank
  }
}
