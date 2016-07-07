package ru.wobot.fc

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import ru.wobot._
import ru.wobot.net.Fetcher
import ru.wobot.net.Fetcher.{Fetch, SuccessFetch}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object FetchJob {
  def main(args: Array[String]) {
    println(s"Start FetchJob at ${DateTime.now()}:")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableSysoutLogging()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val seeds = env.addSource(new FlinkKafkaConsumer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties)).name("FROM-CRAWL-DB")

    seeds.timeWindowAll(Time.seconds(15)).apply((window: TimeWindow, urls: Iterable[String], out: Collector[String]) => {
      val count: Int = urls.count(_ => true)
      out.collect(s"select seeds / per 15 sec = $count")
    })
      .print()
      .name("SPEEDOMETER-GENERATE")

    val fetch = seeds
      .countWindowAll(25)
      .apply((window: GlobalWindow, urls: Iterable[String], out: Collector[Fetch]) => {
        println(s"--Start async fetch: ${System.nanoTime}")
        val all: Future[Seq[Fetch]] = Future.sequence(urls.toSeq.map(u => Fetcher.fetch(u)))
        for (fetch <- Await.result(all, Duration.Inf)) {
          out.collect(fetch)
        }
        println(s"--End async fetch: ${System.nanoTime}")
      }).name("FETCH") //.setParallelism(8)

    val outlinks: DataStream[String] = fetch
      .filter(x => x.isInstanceOf[SuccessFetch[Seq[Long]]])
      .map(x => x.asInstanceOf[SuccessFetch[Seq[Long]]])
      .flatMap(x => x.data).map(x => s"vk://id$x")
      .shuffle
      .name("OUTLINKS")

    outlinks
      .timeWindowAll(Time.seconds(15))
      .apply((window: TimeWindow, urls: Iterable[String], out: Collector[String]) => {
        val count: Int = urls.count(_ => true)
        out.collect(s"outlinks / per 15 sec = $count")
      })
      .print()
      .name("SPEEDOMETER-OUTLINKS")

    outlinks
      .addSink(new FlinkKafkaProducer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties))
      .name("TO-CRAWL-DB")
    
    val startTime = System.nanoTime
    env.execute()
    val elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS)
    Fetcher.close()
    Thread.sleep(1000)
    println(s"FetchJob.ElapsedTime=$elapsedTime ms")
  }

}