package ru.wobot.fc

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import ru.wobot._
import ru.wobot.fc.util.ThroughputLogger
import ru.wobot.net.Fetcher
import ru.wobot.net.Fetcher.{ErrorFetch, Fetch}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}

object FetchJob {
  def main(args: Array[String]) {
    println(s"Start FetchJob at ${DateTime.now()}:")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.enableSysoutLogging()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val seeds: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties)).rebalance.name("FROM-CRAWL-DB")

//    val toFetch: DataStream[Seq[String]] = seeds.keyBy(x => x).timeWindow(Time.seconds(1)).apply((s: String, window: TimeWindow, urls: Iterable[String], out: Collector[Seq[String]]) => {
//      urls.grouped(20).foreach(x => out.collect(x.toSeq))
//    })

//    val toFetch: DataStream[Seq[String]] = seeds.keyBy(x => x.hashCode).countWindow(9).apply((hash: Int, window: GlobalWindow, urls: Iterable[String], out: Collector[Seq[String]]) => {
//      out.collect(urls.toSeq)
//    })

    val toFetch: DataStream[Seq[String]] = seeds.countWindowAll(10).apply((window: GlobalWindow, urls: Iterable[String], out: Collector[Seq[String]]) =>{
      out.collect(urls.toSeq)
    })

    //    val fetch: DataStream[Fetch] = toFetch
    //      .map(x => {
    //        try {
    //          val all = Future.sequence(x.map(Fetcher.fetch(_)))
    //          Await.result(all, Duration.create(1, TimeUnit.SECONDS))
    //        }
    //        catch {
    //          case e: TimeoutException => x.map(x => ErrorFetch(x, e.getMessage))
    //        }
    //      })
    //      .flatMap(x => x)
    //      .name("FETCH")

    val fetch: DataStream[Fetch] = toFetch
      .flatMap((urls: Seq[String], out: Collector[Fetch]) => {
        try {
          val all = Future.sequence(urls.map(Fetcher.fetch(_)))
          Await.result(all, Duration.create(1, TimeUnit.SECONDS)).foreach(x=>out.collect(x))
        }
        catch {
          case e: TimeoutException => urls.foreach(x => out.collect(ErrorFetch(x, e.getMessage)))
        }
      })
      .name("FETCH")

    fetch.flatMap(new ThroughputLogger[Fetch](200, 250))

    fetch.addSink(new FlinkKafkaProducer09[Fetch](FETCHED_TOPIC_NAME, new TypeInformationSerializationSchema[Fetch](TypeInformation.of(classOf[Fetch]), env.getConfig), params.getProperties))
    val startTime = System.nanoTime
    env.execute()
    val elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS)
    Fetcher.close()
    Thread.sleep(1000)
    println(s"FetchJob.ElapsedTime=$elapsedTime ms")
  }

}