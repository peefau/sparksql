package com.inspur.tax

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.inspur.tax.hbase.ClientUtils
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LogStat {
    //集群模式
    val spark = SparkSession
        .builder()
        .appName("Log Statistic")
        .enableHiveSupport()
        .getOrCreate()

    //Get HBase connection
    val conn = ClientUtils.getConnection

    /**
      * 获取指定配置文件的属性key/value
      *
      * @param proPath
      * @return
      */
    def getProperties(proPath: String): Properties = {
        val properties: Properties = new Properties()
        properties.load(new FileInputStream(proPath))
        properties
    }

    /**
      * 以不同的模式（追加、覆盖等）保存DF到指定的表中
      *
      * @param dataFrame
      * @param tableName
      * @param saveMode
      * @param properties
      */
    def save2Oracle(dataFrame: DataFrame, tableName: String, saveMode: SaveMode, properties: Properties): Unit = {
        val table = tableName
        //配置文件中的key带mysql前缀，而spark配置不能带mysql前缀，故按照spark连接数据库的格式配置
        val prop = new Properties()
        prop.setProperty("driver", properties.getProperty("oracle.driver"))
        prop.setProperty("url", properties.getProperty("oracle.url"))
        prop.setProperty("user", properties.getProperty("oracle.username"))
        prop.setProperty("password", properties.getProperty("oracle.password"))
        prop.setProperty("batchsize", properties.getProperty("oracle.batchsize"))
        prop.setProperty("validationQuery",properties.getProperty("oracle.validationQuery"))
        dataFrame
            .write
            .mode(saveMode)
//            .option("truncate", value = true)
            .jdbc(prop.getProperty("url"), table, prop)
    }

    /**
      * 取hbase中的数据，汇总后写入oracle的指定表中
      *
      * @param args
      */
    def main(args: Array[String]): Unit = {
        //Get date of input ,or get current date
        val dt = if (args.length > 0) args(0).toString else {
            val now: Date = new Date()
            val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
            val date = dateFormat.format(now)
            date
        }
        //定义时间格式
        val fmt = "yyyy-MM-dd HH:mm:ss:SSS"
        val start = "1970-01-01 00:00:00:000"
        val currday = "yyyy-MM-dd 00:00:00:000"

        //源表名
        val tableName = "iosp_proxy_log_" + dt
        println("tableName = " + tableName)
        //configuration file
        val proPath = if (args.length > 1) args(1).toString else {
            //spark-submit
            "datasource.properties"
        }
        val properties: Properties = getProperties(proPath)
        //Get HBase Configuration
        val conf = conn.getConfiguration
        //设置查询的表名
        conf.set(TableInputFormat.INPUT_TABLE, tableName)
        val scan = new Scan()
        val max: Long = System.currentTimeMillis + 1
        val interval:Long = properties.getProperty("log.stat.interval").toLong
        val min: Long = max - interval

        conf.set(TableInputFormat.SCAN_TIMERANGE_START,min.toString)
        conf.set(TableInputFormat.SCAN_TIMERANGE_END,max.toString)

        import spark.implicits._
        val hbaseRDD = spark
            .sparkContext
            .newAPIHadoopRDD(conf,
                classOf[TableInputFormat],
                classOf[ImmutableBytesWritable],
                classOf[Result])
        hbaseRDD.cache()
        //将数据映射为表，即将RDD转为DF
        val rdd = hbaseRDD.map(r => (
            Bytes.toString(r._2.getValue(Bytes.toBytes("request"), Bytes.toBytes("tranSeq"))),
            Bytes.toString(r._2.getValue(Bytes.toBytes("request"), Bytes.toBytes("requestIp"))),
            Bytes.toString(r._2.getValue(Bytes.toBytes("request"), Bytes.toBytes("accessKey"))),
            Bytes.toString(r._2.getValue(Bytes.toBytes("request"), Bytes.toBytes("serviceInfo"))),
            Bytes.toString(r._2.getValue(Bytes.toBytes("request"), Bytes.toBytes("serviceCaller"))),
            Bytes.toString(r._2.getValue(Bytes.toBytes("log"), Bytes.toBytes("logStepId"))),
            Bytes.toString(r._2.getValue(Bytes.toBytes("log"), Bytes.toBytes("logTimeIn"))),
            Bytes.toString(r._2.getValue(Bytes.toBytes("log"), Bytes.toBytes("logTimeOut"))),
            Bytes.toString(r._2.getValue(Bytes.toBytes("log"), Bytes.toBytes("logMsgCode"))),
            Bytes.toString(r._2.getValue(Bytes.toBytes("msg"), Bytes.toBytes("msgBody")))
        )).filter(r =>
            r._6.substring(0, 1).equals("0") || r._6.substring(0, 1).equals("2")
        ).toDF("tranSeq"
            , "requestIp"
            , "accessKey"
            , "serviceInfo"
            , "serviceCaller"
            , "logStepId"
            , "logTimeIn"
            , "logTimeOut"
            , "logMsgCode"
            , "msgBody"
        )
        rdd.cache()
        //转为临时表
        rdd.createOrReplaceTempView(tableName)
        val df = spark.sql(properties
            .getProperty("spark_sql")
            .replace("###", tableName)
        )
//        df.show(10)
        val jedisCluster = RedisClient.initJedis(proPath)
        println("Redis Connected ... " + jedisCluster.toString)
        val rkList = df.select("UUID").collectAsList()
        //取得redis中的开关值，即最后一批入库时间
        val lit = jedisCluster.get("LastInTime")
        println("lit=" + lit)
        val lastInTime = if (lit == null) RedisClient.tranTimeToLong(start) else lit.toLong
        //取得当天午夜时间
        val midNight = RedisClient.tranTimeToLong(RedisClient.NowDate(currday))
        //取得当前时间
        val nowTime = RedisClient.tranTimeToLong(RedisClient.NowDate(fmt))
        //最后入库时间<=午夜时间<=当前时间，需要清空redis中的UUID集合
        if (lastInTime.longValue() <= midNight.longValue() &&
            midNight.longValue() <= nowTime.longValue()) {
            val result = jedisCluster.smembers("UUID")
            val it = result.iterator()
            while (it.hasNext) {
                jedisCluster.srem("UUID", it.next())
            }
        }
        //与select结果key进行比较，若存在于集合中，则更新oracle表，否则插入oracle表
        var retStr: Array[String] = Array()
        var str = ""
        for (i <- 0 until rkList.size()) {
            val tmp = rkList.get(i).getString(0)
            //            println("i=" + i + ",value=" + tmp)
            if (!jedisCluster.sismember("UUID", tmp)) {
                //插入，拼接要插入的 UUID
                str = "\"" + tmp + "\""
                retStr = retStr :+ str
                jedisCluster.sadd("UUID", tmp)
            }
        }
//        println(retStr.mkString(","))
        if (retStr.length > 0) {
            val retDF = df.where("UUID in (" + retStr.mkString(",") + ")")
//            retDF.show(10)
            //汇总结果保存到oracle中
            save2Oracle(retDF,
                "LOG_FWDY",
                SaveMode.Append,
                getProperties(proPath))
            println("Append Success!")
        }
        //向redis插入一个开关值，保留最后一批入oracle表的时间
        val lastInsertTime = RedisClient.tranTimeToLong(RedisClient.NowDate(fmt)).toString
        if ("OK".equalsIgnoreCase(jedisCluster.set("LastInTime", lastInsertTime))) {
            println("插入开关值成功")
        }
        spark.close()
        System.exit(0)
    }
}
