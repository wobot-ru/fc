package ru.wobot.fc.print

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.joda.time.DateTime
import ru.wobot.fc.StringGeneratingSourceFunction

object SampleSourceJob {
  def main(args: Array[String]) {
    println(s"Start FetchJob at ${DateTime.now()}:")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.enableSysoutLogging()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)


    val stream: DataStream[String] = env.addSource(new StringGeneratingSourceFunction(80)).startNewChain

//    seeds.timeWindowAll(Time.seconds(5)).apply((window: TimeWindow, urls: Iterable[String], out: Collector[String]) => {
//      val count: Int = urls.count(_ => true)
//      out.collect(s"select seeds / per 5 sec = $count")
//    })
//      .print()


    stream.print().setParallelism(8)
    env.execute()
 }


}