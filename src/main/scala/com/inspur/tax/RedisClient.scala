package com.inspur.tax

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties, UUID}

import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

object RedisClient extends Serializable {

    def NowDate(fmt: String): String = {
        val now: Date = new Date()
        val dateFormat: SimpleDateFormat = new SimpleDateFormat(fmt)
        val date = dateFormat.format(now)
        date
    }

    def tranTimeToLong(time: String): Long = {
        val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
        val dt = fm.parse(time)
        val aa = fm.format(dt)
        println(aa)
        val tim: Long = dt.getTime
        tim
    }

//    def main(args: Array[String]): Unit = {
//        //配置文件
//        val propPath = "/Users/pingfuli/Desktop/kerberos/datasource.properties"
//        val jedis = initJedis(propPath)
//
//        //set start
//        val uid = UUID.randomUUID().toString.replaceAll("-", "")
//        jedis.sadd("UUID", uid)
//        println(jedis.scard("UUID") + "," + jedis.smembers("UUID"))
//
//        val start = tranTimeToLong(NowDate("yyyy-MM-dd 00:00:00:000"))
//        val end = tranTimeToLong(NowDate("yyyy-MM-dd HH:mm:ss:SSS"))
//        val diff = end - start
//
//        //清空昨天的数据
//        if (diff > 0) {
//            val result = jedis.smembers("UUID")
//            val it = result.iterator()
//            while (it.hasNext) {
//                jedis.srem("UUID", it.next())
//            }
//        }
//        System.exit(1)
//    }

    def getProPerties(proPath: String): Properties = {
        val properties: Properties = new Properties()
        properties.load(new FileInputStream(proPath))
        properties
    }

    def initJedis(proPath:String): JedisCluster = {
        println("proPath="+proPath)
        val clusterNodes = new util.HashSet[HostAndPort]()
        val prop:Properties = getProPerties(proPath)
        val hostAndPort = prop.getProperty("redis_cluster")
        val hosts = hostAndPort.split(",")
        for( x <- hosts) {
            val host = x.split(":")(0)
            val port = x.split(":")(1).toInt
            clusterNodes.add(new HostAndPort(host,port))
            println("host="+host+",port="+port)
        }
        val jedisConfig = new JedisPoolConfig
        jedisConfig.setMaxIdle(prop.getProperty("redis.maxIdle").toInt)
        jedisConfig.setMaxTotal(prop.getProperty("redis.maxTotal").toInt)
        jedisConfig.setMaxWaitMillis(prop.getProperty("redis.maxWaitMillis").toInt)

        lazy val jedisCluster = new JedisCluster(clusterNodes,
            prop.getProperty("redis.connectionTimeout").toInt,
            prop.getProperty("redis.soTimeout").toInt,
            prop.getProperty("redis.maxAttempts").toInt,
            prop.getProperty("redis.password"),
            jedisConfig
        )
        jedisCluster
    }
}