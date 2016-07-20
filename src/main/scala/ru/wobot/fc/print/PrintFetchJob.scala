package ru.wobot.fc.print

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import ru.wobot._
import ru.wobot.net.Fetcher.Fetch

object PrintFetchJob {
  def main(args: Array[String]) {
    println(s"Start FetchJob at ${DateTime.now()}:")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.enableSysoutLogging()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val fetch: DataStream[Fetch] = env.addSource(new FlinkKafkaConsumer09[Fetch](FETCHED_TOPIC_NAME, new TypeInformationSerializationSchema[Fetch](TypeInformation.of(classOf[Fetch]), env.getConfig), params.getProperties))

    fetch.timeWindowAll(Time.seconds(30)).apply((window: TimeWindow, urls: Iterable[Fetch], out: Collector[String]) => {
      val count: Int = urls.count(_ => true)
      out.collect(s"fetch / per 30 sec = $count")
    })
      .print()


    fetch.map(x=>x.uri).rebalance.writeAsText("file:///c:\\tmp\\uri")


    fetch.map((_, 1))
      .keyBy(x=>x._1.uri)
      .timeWindow(Time.minutes(1))
      .sum(1)
      .keyBy(1)
      .timeWindow(Time.minutes(1))
      .apply((tuple: Tuple, window: TimeWindow, seeds: Iterable[(Fetch, Int)], out: Collector[String]) => {
        seeds.toSeq.sortBy(_._2)(Ordering[Int].reverse).take(10).foreach(x => out.collect(s"${x._1.uri} ${x._2}"))
      })
      .print()

    env.execute()
  }

}