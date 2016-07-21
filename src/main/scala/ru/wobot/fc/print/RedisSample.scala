package ru.wobot.fc.print

import java.util.concurrent.TimeUnit

import com.redis.serialization.Parse.Implicits._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import ru.wobot._
import ru.wobot.net.RedisConnection

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object RedisSample {
  def main(args: Array[String]) {
    println(s"Start FetchJob at ${DateTime.now()}:")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.enableSysoutLogging()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)


    val seeds: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String](CRAWL_TOPIC_NAME, new TypeInformationSerializationSchema[String](TypeInformation.of(classOf[String]), env.getConfig), params.getProperties)).rebalance.name("FROM-CRAWL-DB")



    var s = seeds.flatMap(new FlatMapFunction[String, Long]() {
      override def flatMap(t: String, collector: Collector[Long]): Unit = {
        RedisConnection.conn.withClient(x => {
          val crawlDate: Future[Option[Long]] = Future {
            x.get[Long](t)
          }
          val i = Await.result(crawlDate, Duration.create(1, TimeUnit.SECONDS))
          if (i.isEmpty)
            collector.collect(0)
          else collector.collect(i.get)
        })
      }
    })

//    var s = seeds.flatMap(new FlatMapFunction[String, Int]() {
//      override def flatMap(t: String, collector: Collector[Int]): Unit = {
//        RedisConnection.conn.withClient(x => {
//          x.set(t,System.nanoTime())
//        })
//      }
//    })

    s.print()
    env.execute()
    RedisConnection.conn.close
  }
}