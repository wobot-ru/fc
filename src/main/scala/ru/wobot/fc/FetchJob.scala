package ru.wobot.fc

import java.util.concurrent.TimeUnit

import com.redis.RedisClientPool
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.DateTime
import ru.wobot._
import ru.wobot.fc.util.ThroughputLogger
import ru.wobot.net.Fetcher
import ru.wobot.net.Fetcher.{ErrorFetch, Fetch, SuccessFetch}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}

object FetchJob {
  def main(args: Array[String]) {
    println(s"Start FetchJob at ${DateTime.now()}:")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.enableSysoutLogging()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val seeds: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties)).keyBy(x => x)

    val toFetch: DataStream[Seq[String]] = seeds.countWindowAll(20).apply((window: GlobalWindow, urls: Iterable[String], out: Collector[Seq[String]]) => {
      out.collect(urls.toSeq)
    })

    val fetch: DataStream[Fetch] = toFetch
      .flatMap((urls: Seq[String], out: Collector[Fetch]) => {
        try {
          val all = Future.sequence(urls.map(Fetcher.fetch(_)))
          val result: Seq[Fetch] = Await.result(all, Duration.create(1, TimeUnit.SECONDS))
          result.foreach(out.collect(_))
          RedisConnection.conn.withClient(x => {
            result.foreach(f => x.set(f.uri, f.crawlDate))
          })
        }
        catch {
          case e: TimeoutException => {
            val crawlDate = System.nanoTime
            urls.foreach(x => out.collect(ErrorFetch(x, crawlDate, e.getMessage)))
          }
        }
      })
      .rebalance
      .name("FETCH")

    fetch.flatMap(new ThroughputLogger[Fetch](200, 50000))

    fetch.print()

    fetch.addSink(new FlinkKafkaProducer09[Fetch](FETCHED_TOPIC_NAME, new TypeInformationSerializationSchema[Fetch](TypeInformation.of(classOf[Fetch]), env.getConfig), params.getProperties))
    //    fetch.writeUsingOutputFormat(new HBaseOutputFormat[Fetch](FETCHED_TOPIC_NAME, x =>
    //      new Put(Bytes.toBytes(x.uri)).add(Bytes.toBytes("id"), Bytes.toBytes("uri"), Bytes.toBytes(x.uri))))

    val startTime = System.nanoTime
    env.execute()
    val elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS)
    Fetcher.close()
    RedisConnection.conn.close
    println(s"FetchJob.ElapsedTime=$elapsedTime ms")
    Thread.sleep(1000)
  }

  object RedisConnection extends Serializable {
    lazy val conn: RedisClientPool = new RedisClientPool("localhost", 6379)
  }

}