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

    val toFetch: DataStream[Seq[String]] = seeds.map(x => (x, Random.nextInt(8))).keyBy(1).timeWindow(Time.seconds(1)).apply((tuple: Tuple, window: TimeWindow, urls: Iterable[(String, Int)], out: Collector[Seq[String]]) =>{
      urls.map(x=>x._1).toSeq.distinct.grouped(10).foreach(x => out.collect(x))
    }).rebalance.setParallelism(8)

    toFetch.print()
    val fetch: DataStream[Fetch] = toFetch.flatMap((urls: Seq[String], out: Collector[Fetch]) => {
      //println(s"--topN.map - Start async fetch: ${System.nanoTime} | ThreadId=${Thread.currentThread().getId}")
      val all = Future.sequence(urls.map(Fetcher.fetch(_)))
      try {
        for (fetch <- Await.result(all, Duration.create(2, TimeUnit.SECONDS))) {
          out.collect(fetch)
        }
      }
      catch {
        case e: TimeoutException => ()
      }
      //println(s"--topN.map - End async fetch: ${System.nanoTime} | ThreadId=${Thread.currentThread().getId}")
    }).rebalance.setParallelism(8).name("FETCH")


    //    val fetch = seeds.shuffle.map(x => {
    //      try {
    //        Await.result(Fetcher.fetch(x), Duration.create(1, TimeUnit.SECONDS))
    //      }
    //      catch {
    //        case e: TimeoutException => ErrorFetch(x, e.getMessage)
    //      }
    //    }).rebalance.setParallelism(8)
    //    //fetch.print()
    //
//    fetch
//      .writeUsingOutputFormat(new HBaseOutputFormat[Fetch](CRAWL_TOPIC_NAME, p => new Put(Bytes.toBytes(s"${p.uri}")).add(Bytes.toBytes("id"), Bytes.toBytes("date"), Bytes.toBytes(System.nanoTime()))))
//        .setParallelism(8)
//      .name("TO-HBASE")

    val outlinks: DataStream[String] = fetch
      .filter(x => x.isInstanceOf[SuccessFetch[Seq[Long]]])
      .map(x => x.asInstanceOf[SuccessFetch[Seq[Long]]])
      .flatMap(x => x.data)
      .map(x => s"vk://id$x")
      .rebalance
      .name("OUTLINKS")

    val topBatch = outlinks
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.milliseconds(10000))
      .sum(1)
      .setParallelism(8)
      .keyBy(1)
      .timeWindow(Time.milliseconds(10000))
      .apply((tuple: Tuple, window: TimeWindow, seeds: Iterable[(String, Int)], out: Collector[Seq[String]]) => {
        //        val by: Seq[(String, Int)] = seeds.toSeq.sortBy(_._2)(Ordering[Int].reverse)
        //        val toList: List[(String, Int)] = by.toList
        //        val count: Int = toList.count(x => true)
        //        val map: Seq[String] = by.take(100).map(_._1)
        //        val count: Int = seeds.count(_ => true)
        //        println(count)
        out.collect(seeds.toSeq.sortBy(_._2)(Ordering[Int].reverse).take(100).map(_._1))
      })
      .setParallelism(8)

    val topN: DataStream[String] = topBatch.flatMap(x => x).rebalance

    //    topN
    //      .timeWindowAll((Time.seconds(1)))
    //      .apply((window: TimeWindow, urls: Iterable[String], out: Collector[String]) => {
    //        //println(urls)
    //        val count: Int = urls.count(_ => true)
    //        out.collect(s"topN / per sec = $count")
    //      })
    //      .print()


    val unfetch: DataStream[String] = seeds
      .coGroup(topN)
      .where(x => x)
      .equalTo(x => x)
      .window(TumblingEventTimeWindows.of(Time.seconds(15)))
      //.window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
      .apply((left: Iterator[String], right: Iterator[String], out: Collector[String]) => {
      if (left.isEmpty) {
        right.foreach(out.collect(_))
      }
      if (right.isEmpty) {
        left.foreach(out.collect(_))
      }
    })
      .setParallelism(8)
      .rebalance

    //unfetch.print()

    unfetch.addSink(new FlinkKafkaProducer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties))

    val startTime = System.nanoTime
    env.execute()
    val elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS)
    Fetcher.close()
    Thread.sleep(1000)
    println(s"FetchJob.ElapsedTime=$elapsedTime ms")
  }

}