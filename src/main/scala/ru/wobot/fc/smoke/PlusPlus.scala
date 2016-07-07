package ru.wobot.fc.smoke

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
import ru.wobot._
import ru.wobot.net.Fetcher
import ru.wobot.net.Fetcher.{Fetch, SuccessFetch}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object PlusPlus {
  val TOPIC_NAME = "FC-SMOKES-PLUS-PLUS"

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableSysoutLogging()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val init: DataStream[Long] = env.fromElements(1L)
    init.addSink(new FlinkKafkaProducer09[Long](TOPIC_NAME, new TypeInformationSerializationSchema[Long](TypeInformation.of(classOf[Long]), env.getConfig), params.getProperties)).disableChaining()
    env.execute()

    val ping = env.addSource(new FlinkKafkaConsumer09[Long](TOPIC_NAME, new TypeInformationSerializationSchema[Long](TypeInformation.of(classOf[Long]), env.getConfig), params.getProperties)).name("PING")


    ping.timeWindowAll(Time.seconds(15)).apply((window: TimeWindow, urls: Iterable[Long], out: Collector[String]) => {
      val count: Int = urls.count(_ => true)
      out.collect(s"message / per 15 sec = $count")
    }).print()

    val pong: DataStream[Long] = ping.map(x => x + 1)
    pong.addSink(new FlinkKafkaProducer09[Long](TOPIC_NAME, new TypeInformationSerializationSchema[Long](TypeInformation.of(classOf[Long]), env.getConfig), params.getProperties)).disableChaining()

    val startTime = System.nanoTime
    env.execute()
    val elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS)
    Fetcher.close()
    Thread.sleep(1000)
    println(s"elapsedTime=$elapsedTime ms")
  }

}