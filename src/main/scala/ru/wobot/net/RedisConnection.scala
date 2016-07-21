package ru.wobot.net

import com.redis.RedisClientPool

object RedisConnection extends Serializable {
//  lazy val conn: RedisClientPool = new RedisClientPool("hdp-03", 6380)
  lazy val conn: RedisClientPool = new RedisClientPool("localhost", 6379)
}
