package ru.wobot.fc

import java.util.concurrent.TimeUnit

import com.redis.serialization.Parse.Implicits.parseLong
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import ru.wobot._
import ru.wobot.net.Fetcher.{Fetch, SuccessFetch}
import ru.wobot.net.{Fetcher, RedisConnection}

import scala.concurrent._

object ParseJob {
  private val LOGGER = LoggerFactory.getLogger(ParseJob.getClass)

  def main(args: Array[String]) {
    println(s"Start FetchJob at ${DateTime.now()}:")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.enableSysoutLogging()
    //env.disableOperatorChaining()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val fetch: DataStream[Fetch] = env.addSource(new FlinkKafkaConsumer09[Fetch](FETCHED_TOPIC_NAME, new TypeInformationSerializationSchema[Fetch](TypeInformation.of(classOf[Fetch]), env.getConfig), params.getProperties)).keyBy(x => x.uri)

    val outlinks: DataStream[String] = fetch
      .flatMap((fetch: Fetch, collector: Collector[Profile]) => fetch match {
        case f: SuccessFetch[Profile] => collector.collect(f.data)
        case _ => ()
      })
      .flatMap(_.friends)
      .map(x => s"vk://id$x")
      .rebalance
      .name("OUTLINKS")

    //    val outlinksBatch = outlinks.countWindowAll(SIZE_OF_RADIS_BATCH).apply((window: GlobalWindow, urls: Iterable[String], out: Collector[Seq[String]]) => {
    //      out.collect(urls.toSeq)
    //    })

    val unfetch: DataStream[String] = outlinks.flatMap(new FlatMapFunction[String, String]() {
      override def flatMap(url: String, collector: Collector[String]): Unit = {
        try {
          RedisConnection.conn.withClient(x => {
            if (x.get[Long](url).isEmpty) collector.collect(url)
          })
        }
        catch {
          case e: TimeoutException => {
            LOGGER.error("Raddis error", e)
            collector.collect(url)
          }
        }
      }
    })


    //    val unfetch: DataStream[String] = outlinksBatch.flatMap(new FlatMapFunction[Seq[String], String]() {
    //      override def flatMap(urls: Seq[String], collector: Collector[String]): Unit = {
    //        try {
    //          RedisConnection.conn.withClient(x => {
    //
    //            val future = Future.sequence(urls.map(u => Future {
    //              (u, x.get[Long](u))
    //            }))
    //
    //            val res: Seq[(String, Option[Long])] = Await.result[Seq[(String, Option[Long])]](future, Duration.create(5, TimeUnit.SECONDS))
    //            res
    //              .filter(_._2.isEmpty)
    //              .map(x => x._1)
    //              .foreach(collector.collect(_))
    //          })
    //        }
    //        catch {
    //          case e: Throwable => {
    //            LOGGER.error("Raddis error", e)
    //            urls.foreach(collector.collect(_))
    //          }
    //        }
    //      }
    //    }).rebalance


    unfetch.addSink(new FlinkKafkaProducer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties))

    //    val topBatch = unfetch
    //      .map((_, 1))
    //      .keyBy(0)
    //      .timeWindow(Time.hours(1), slideTime)
    //      .sum(1)
    //      .keyBy(1)
    //      .timeWindow(Time.hours(1), slideTime)
    //      .apply((tuple: Tuple, window: TimeWindow, seeds: Iterable[(String, Int)], out: Collector[Seq[String]]) => {
    //        //        val by: Seq[(String, Int)] = seeds.toSeq.sortBy(_._2)(Ordering[Int].reverse)
    //        //        val toList: List[(String, Int)] = by.toList
    //        //        val count: Int = toList.count(x => true)
    //        //        val map: Seq[String] = by.take(100).map(_._1)
    //        //        val count: Int = seeds.count(_ => true)
    //        //        println(count)
    //        out.collect(seeds.toSeq.sortBy(_._2)(Ordering[Int].reverse).take(1).map(_._1))
    //      })
    //
    //    val topN: DataStream[String] = topBatch.flatMap(x => x)
    //
    //    topN.flatMap(new ThroughputLogger[String](32, 100))
    //    //topN.print()
    //    //
    //    topN.addSink(new FlinkKafkaProducer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties))

    val startTime = System.nanoTime
    env.execute("parse")
    val elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS)
    println(s"FetchJob.ElapsedTime=$elapsedTime ms")
    Thread.sleep(1000)
    Fetcher.close()
    RedisConnection.conn.close
  }
}