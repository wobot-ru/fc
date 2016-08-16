package ru.wobot.fc.smoke

import java.util
import java.util.concurrent.TimeUnit

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri, RichSearchResponse}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.search.SearchHit
import ru.wobot.net.Fetcher

object ElasticSmoke {

  def main(args: Array[String]) {
    val startTime = System.nanoTime
    //val settings = Settings.builder().put("cluster.name", "wobot-new-cluster").build()
    val settings = Settings.builder().put("cluster.name", "kviz-es").build()

    val client = ElasticClient.transport(settings, "elasticsearch://192.168.1.121:9300")
    //val client = ElasticClient.transport(settings, "elasticsearch://username:password@192.168.1.121:9300")
    val r: RichSearchResponse = client.execute {
      search in "wobot" -> "post" query "я"
    }.await

    val hits: Array[SearchHit] = r.getHits.getHits
    for (hit<-hits){
      val asMap: util.Map[String, AnyRef] = hit.sourceAsMap()
      val string: String = hit.sourceAsString()
      println(hit.getId)
    }

    val s: String = hits(0).sourceAsMap().getOrDefault("name", "-").asInstanceOf[String]
    println(r.totalHits)

    val elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS)
    Fetcher.close()
    Thread.sleep(1000)
    println(s"elapsedTime=$elapsedTime ms")
  }

}