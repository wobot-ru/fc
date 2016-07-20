package ru.wobot.fc

import java.util.concurrent.TimeUnit

import com.redis.RedisClientPool
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.DateTime
import ru.wobot._
import ru.wobot.fc.util.ThroughputLogger
import ru.wobot.net.Fetcher
import ru.wobot.net.Fetcher.{Fetch, SuccessFetch}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.Random
import com.redis.serialization.Parse.Implicits.parseLong

object ParseJob {
  def main(args: Array[String]) {
    println(s"Start FetchJob at ${DateTime.now()}:")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.enableSysoutLogging()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val fetch: DataStream[Fetch] = env.addSource(new FlinkKafkaConsumer09[Fetch](FETCHED_TOPIC_NAME, new TypeInformationSerializationSchema[Fetch](TypeInformation.of(classOf[Fetch]), env.getConfig), params.getProperties)).keyBy(x => x.uri)

    val successFetched: DataStream[SuccessFetch[Seq[Long]]] = fetch
      .filter(x => x.isInstanceOf[SuccessFetch[Seq[Long]]])
      .filter(_.isInstanceOf[SuccessFetch[Seq[Long]]])
      .map(x => x.asInstanceOf[SuccessFetch[Seq[Long]]])

    //    successFetched.writeUsingOutputFormat(new HBaseOutputFormat[SuccessFetch[Seq[Long]]](FETCHED_TOPIC_NAME, x =>
    //      new Put(Bytes.toBytes(x.uri)).add(Bytes.toBytes("id"), Bytes.toBytes("uri"), Bytes.toBytes(x.uri))))

    val outlinks: DataStream[String] = successFetched
      .flatMap(x => x.data)
      .map(x => s"vk://id$x")
      .name("OUTLINKS")


    val unfetch = outlinks.flatMap(new FlatMapFunction[String, String]() {
      override def flatMap(t: String, collector: Collector[String]): Unit = {
        RedisConnection.conn.withClient(x => {
          val res: Option[Long] = x.get[Long](t)
          if (res.isEmpty)
            collector.collect(t)

          //          val crawlDate: Future[Option[Long]] = Future {
          //            x.get[Long](t)
          //          }
          //          val i = Await.result(crawlDate, Duration.create(1, TimeUnit.SECONDS))
          //          if (i.isEmpty)
          //            collector.collect(t)
        })
      }
    }).rebalance


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
    env.execute()
    val elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS)
    Fetcher.close()
    RedisConnection.conn.close
    Thread.sleep(1000)
    println(s"FetchJob.ElapsedTime=$elapsedTime ms")
  }

  object RedisConnection extends Serializable {
    lazy val conn: RedisClientPool = new RedisClientPool("localhost", 6379)
  }

}