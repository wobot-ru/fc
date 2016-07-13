package ru.wobot.fc

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.DateTime
import ru.wobot._
import ru.wobot.net.Fetcher
import ru.wobot.net.Fetcher.{ErrorFetch, Fetch, SuccessFetch}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.Random

object FetchJob {
  def main(args: Array[String]) {
    println(s"Start FetchJob at ${DateTime.now()}:")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.enableSysoutLogging()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val seeds: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties)).rebalance.name("FROM-CRAWL-DB")

    //    seeds.timeWindowAll(Time.seconds(1)).apply((window: TimeWindow, urls: Iterable[String], out: Collector[String]) => {
    //      //println(s"--SPEEDOMETER | ThreadId=${Thread.currentThread().getId}")
    //      val count: Int = urls.count(_ => true)
    //      out.collect(s"select seeds / per sec = $count")
    //    })
    //      .print()

    //    seeds.map(x => (x, Random.nextInt(8))).setParallelism(8).keyBy(1).timeWindow(Time.seconds(1)).apply((tuple: Tuple, window: TimeWindow, urls: Iterable[(String, Int)], out: Collector[Seq[String]]) =>{
    //      urls.toSeq.groupBy((tuple: (String, Int)) => tuple._2).
    //    })

    val toFetch: DataStream[Seq[String]] = seeds.map(x => (x, Random.nextInt(8))).keyBy(1).timeWindow(Time.seconds(1)).apply((tuple: Tuple, window: TimeWindow, urls: Iterable[(String, Int)], out: Collector[Seq[String]]) => {
      urls.map(x => x._1).toSeq.distinct.grouped(10).foreach(x => out.collect(x))
    })

    val fetch: DataStream[Fetch] = toFetch.flatMap((urls: Seq[String], out: Collector[Fetch]) => {
      //println(s"--topN.map - Start async fetch: ${System.nanoTime} | ThreadId=${Thread.currentThread().getId}")
      val all = Future.sequence(urls.map(Fetcher.fetch(_)))
      try {
        for (fetch <- Await.result(all, Duration.create(3, TimeUnit.SECONDS))) {
          out.collect(fetch)
        }
      }
      catch {
        case e: TimeoutException => urls.map(x => ErrorFetch(x, e.getMessage)).foreach(x=>out.collect(x))
      }
      //println(s"--topN.map - End async fetch: ${System.nanoTime} | ThreadId=${Thread.currentThread().getId}")
    }).name("FETCH")

    fetch.timeWindowAll(Time.seconds(1)).apply((window: TimeWindow, urls: Iterable[Fetch], out: Collector[String]) => {
      //println(s"--SPEEDOMETER | ThreadId=${Thread.currentThread().getId}")
      val count: Int = urls.count(_ => true)
      out.collect(s"fetch / per sec = $count")
    })
      .print()

    fetch.addSink(new FlinkKafkaProducer09[Fetch](FETCHED_TOPIC_NAME, new TypeInformationSerializationSchema[Fetch](TypeInformation.of(classOf[Fetch]), env.getConfig), params.getProperties))
    val startTime = System.nanoTime
    env.execute()
    val elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS)
    Fetcher.close()
    Thread.sleep(1000)
    println(s"FetchJob.ElapsedTime=$elapsedTime ms")
  }

}