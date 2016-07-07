package ru.wobot.fc

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
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
import scala.util.Random

object FetchJob {
  def main(args: Array[String]) {
    println(s"Start FetchJob at ${DateTime.now()}:")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.enableSysoutLogging()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val seeds: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties)).name("FROM-CRAWL-DB")

    seeds.timeWindowAll(Time.seconds(10)).apply((window: TimeWindow, urls: Iterable[String], out: Collector[String]) => {
      println(s"--SPEEDOMETER | ThreadId=${Thread.currentThread().getId}")
      val count: Int = urls.count(_ => true)
      out.collect(s"select seeds / per 10 sec = $count")
    })
      .print()
      .name("SPEEDOMETER-SEEDS")

    val emitSeeds: DataStream[(String, Int)] = seeds
      .map {
        (_, 1)
      }
      .keyBy(0)
      .timeWindow(Time.seconds(3L), Time.seconds(1L))
      .sum(1)


    val topN: DataStream[Seq[String]] = emitSeeds
      .timeWindowAll(Time.seconds(1))
      .apply((window: TimeWindow, seeds: Iterable[(String, Int)], out: Collector[Seq[String]]) =>
        out.collect(seeds.toSeq.sortBy(_._2)(Ordering[Int].reverse).take(10).map(_._1))
      ).name("TOP-N URLS")


    val fetch: DataStream[Fetch] = topN.flatMap((urls: Seq[String], out: Collector[Fetch]) => {
      println(s"--topN.map - Start async fetch: ${System.nanoTime} | ThreadId=${Thread.currentThread().getId}")
      val all: Future[Seq[Fetch]] = Future.sequence(urls.map(Fetcher.fetch(_)))
      for (fetch <- Await.result(all, Duration.Inf)) {
        out.collect(fetch)
      }
      println(s"--topN.map - End async fetch: ${System.nanoTime} | ThreadId=${Thread.currentThread().getId}")
    }).name("FETCH")

    val outlinks: DataStream[String] = fetch
      .filter(x => x.isInstanceOf[SuccessFetch[Seq[Long]]])
      .map(x => x.asInstanceOf[SuccessFetch[Seq[Long]]])
      .flatMap(x => x.data).map(x => s"vk://id$x")
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