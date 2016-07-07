package ru.wobot.fc

import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.utils.ParameterTool
import ru.wobot.CLI

object CrawlJob {
  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)
    val startTime = System.nanoTime
    if (params.has(CLI.INJECT_KEY))
      InjectJob.main(args)
    if (params.has(CLI.FETCH_KEY))
      FetchJob.main(args)

    val elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime - startTime, TimeUnit.NANOSECONDS)
    Thread.sleep(1000)
    println(s"CrawlJob.ElapsedTime=$elapsedTime ms")
  }

}