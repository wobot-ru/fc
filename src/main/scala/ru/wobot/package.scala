package ru

import java.io.IOException

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.hbase.HBaseConfiguration

package object wobot {
  val CRAWL_TOPIC_NAME = "FC-CRAWL-DB"
  val FETCHED_TOPIC_NAME = "FC-FETCHED"

  class HBaseOutputFormat[T](tableName: String, write: T => Put) extends OutputFormat[T] {
    private var conf: HConf = null
    private var table: HTable = null

    private val serialVersionUID: Long = 1L

    override def configure(parameters: Configuration): Unit = conf = HBaseConfiguration.create

    @throws[IOException]
    override def open(taskNumber: Int, numTasks: Int) {
      table = new HTable(conf, tableName)
    }

    @throws[IOException]
    override def writeRecord(d: T) {
      table.put(write(d))
      table.flushCommits
    }

    @throws[IOException]
    override def close {
      table.flushCommits
      table.close
    }
  }

}
