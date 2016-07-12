package ru.wobot.fc

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
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

    val elements = env.fromElements(1, 2, 3)

    val seeds: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties)).rebalance.name("FROM-CRAWL-DB")

    //seeds.print()
    //    env.execute()
    val speedometerTime: Time = Time.seconds(10)
    val slideTime: Time = Time.seconds(2)

    //    seeds.timeWindowAll(slideTime).apply((window: TimeWindow, urls: Iterable[String], out: Collector[String]) => {
    //      //println(s"--SPEEDOMETER | ThreadId=${Thread.currentThread().getId}")
    //      val count: Int = urls.count(_ => true)
    //      out.collect(s"select seeds / per 2 sec = $count")
    //    })
    //      .print()
    //      .name("SPEEDOMETER-SEEDS")
    //
    //
    //    val emitSeeds: DataStream[(String, Int)] = seeds
    //      .map {
    //        (_, 1)
    //      }
    //      .keyBy(0)
    //      .timeWindow(speedometerTime, slideTime)
    //      .sum(1)
    //
    //
    //    val topN: DataStream[Seq[String]] = emitSeeds
    //      .timeWindowAll(slideTime)
    //      .apply((window: TimeWindow, seeds: Iterable[(String, Int)], out: Collector[Seq[String]]) =>
    //        out.collect(seeds.toSeq.sortBy(_._2)(Ordering[Int].reverse).take(25).map(_._1))
    //      ).name("TOP-N URLS")
    //
    //
    //    val fetch: DataStream[Fetch] = topN.flatMap((urls: Seq[String], out: Collector[Fetch]) => {
    //      //println(s"--topN.map - Start async fetch: ${System.nanoTime} | ThreadId=${Thread.currentThread().getId}")
    //      val all = Future.sequence(urls.map(Fetcher.fetch(_)))
    //      for (fetch <- Await.result(all, Duration.Inf)) {
    //        out.collect(fetch)
    //      }
    //      //println(s"--topN.map - End async fetch: ${System.nanoTime} | ThreadId=${Thread.currentThread().getId}")
    //    }).name("FETCH")


    val fetch = seeds.map(x => {
      try {
        Await.result(Fetcher.fetch(x), Duration.create(2, TimeUnit.SECONDS))
      }
      catch {
        case e: TimeoutException => ErrorFetch(x, e.getMessage)
      }
    })

    fetch
      .writeUsingOutputFormat(new HBaseOutputFormat[Fetch](CRAWL_TOPIC_NAME, p => new Put(Bytes.toBytes(s"${p.uri}")).add(Bytes.toBytes("id"), Bytes.toBytes("date"), Bytes.toBytes(System.nanoTime()))))
      .name("TO-HBASE")

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
      .timeWindow(Time.milliseconds(5000))
      .sum(1)
      .setParallelism(8)
      .keyBy(1)
      .timeWindow(Time.milliseconds(5000))
      .apply((tuple: Tuple, window: TimeWindow, seeds: Iterable[(String, Int)], out: Collector[Seq[String]]) => {
        //        val by: Seq[(String, Int)] = seeds.toSeq.sortBy(_._2)(Ordering[Int].reverse)
        //        val toList: List[(String, Int)] = by.toList
        //        val count: Int = toList.count(x => true)
        //        val map: Seq[String] = by.take(100).map(_._1)
        //        println(count)
        out.collect(seeds.toSeq.sortBy(_._2)(Ordering[Int].reverse).take(25).map(_._1))
      })
      .setParallelism(8)

    val topN: DataStream[String] = topBatch.flatMap(x => x)

    topN
      .keyBy(x => x)
      .timeWindowAll(Time.seconds(2))
      .apply((window: TimeWindow, urls: Iterable[String], out: Collector[String]) =>{
        //println(urls)
        val count: Int = urls.count(_ => true)
        out.collect(s"topN / per 15 sec = $count")
      })
      .setParallelism(8)
      .print()
      .name("SPEEDOMETER-topn")


    val unfetch: DataStream[String] = seeds
      .coGroup(topN)
      .where(x => x)
      .equalTo(x => x)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(50)))
      .apply((left: Iterator[String], right: Iterator[String], out: Collector[String]) => {
        if (right.isEmpty) {
          left.foreach(out.collect(_))
        }
      })

    unfetch.print()

    //
    //
    //

    //
    //

    //
    //    //StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
    //
    //    topN.print()
    //
    ////    val unfetch: DataStream[String] = seeds.coGroup(topN).where(x => x).equalTo(x => x).window(TumblingEventTimeWindows.of(slideTime))
    ////      .apply((left: Iterator[String], right: Iterator[String], out: Collector[String]) => {
    ////        if (right.isEmpty) {
    ////          left.foreach(out.collect(_))
    ////        }
    ////      })
    //
    //    //    topN.join(seeds).where(0).equalTo(0).window().addSink(new FlinkKafkaProducer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties))
    //    //      .name("TO-CRAWL-DB")

    val startTime = System.nanoTime
    env.execute()
    val elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS)
    Fetcher.close()
    Thread.sleep(1000)
    println(s"FetchJob.ElapsedTime=$elapsedTime ms")
  }

}