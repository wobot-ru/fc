package ru.wobot.fc.print

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{createTypeInformation => _, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import ru.wobot._
import ru.wobot.net.Fetcher.Fetch
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{Row, Table, TableEnvironment}
import org.apache.flink.api.java.tuple.Tuple1

object CountTotalFetchedJob {

  def main(args: Array[String]) {
    println(s"Start FetchJob at ${DateTime.now()}:")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.enableSysoutLogging()
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    val fetch: DataStream[Fetch] = env.addSource(new FlinkKafkaConsumer09[Fetch](FETCHED_TOPIC_NAME, new TypeInformationSerializationSchema[Fetch](TypeInformation.of(classOf[Fetch]), env.getConfig), params.getProperties)).rebalance.name("FROM-CRAWL-DB")

//    fetch.timeWindowAll(Time.seconds(1)).apply((window: TimeWindow, urls: Iterable[Fetch], out: Collector[String]) => {
//      val count: Int = urls.count(_ => true)
//      out.collect(s"fetch / per sec = $count")
//    })
//      .print()

    val map: DataStream[Tuple1[String]] = fetch.map(x=>new Tuple1[String](x.uri))
    val table = map.toTable(tEnv, 'a)
    val stream: DataStream[Row] = table.toDataStream[Row]
    stream.print()
//    val collect: Seq[Row] = table.collect()
//
//    println(collect)
    //    fetch.print()
    env.execute()
 }

}