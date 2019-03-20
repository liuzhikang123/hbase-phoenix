package com.zyuc.streaming

import com.zyuc.dpi.utils.FileUtils
import com.zyuc.streaming.KafkaHbaseManager._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import scala.collection.mutable
import scala.util.parsing.json.JSON
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object IotRadiusHa {


  def main(args: Array[String]): Unit = {

    val processInterval = 5
    val brokers = "10.37.7.139:9092,10.37.7.140:9092,10.37.7.141:9092"
    val zookeeperQuorum = "10.37.7.139:2181,10.37.7.140:2181,10.37.7.141:2181"
    val topicName = "haradius_out"
    val topics = Array(topicName)

    //val spark = SparkSession.builder().master("local[2]").appName("test").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sparkConf = spark.sparkContext.getConf
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outputPath = sparkConf.get("spark.app.outputPath", "hdfs://10.37.28.38:8020/user/slview/hadoop/haradius/")
    val groupId = sparkConf.get("spark.app.groupId", "groupSpark")
    // kafka params
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",//earliest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val partitions = "d,h,m5"

    def getTemplate: String = {
      var template = ""
      val partitionArray = partitions.split(",")
      for (i <- 0 until partitionArray.length)
        template = template + "/" + partitionArray(i) + "=*"
      template
    }


    val ssc = new StreamingContext(spark.sparkContext, Seconds(processInterval))


    // hbase 建表语句 create 'spark_kafka_radius_offsets', {NAME=>'offsets', TTL=>2592000}
    val hbaseTableName = "spark_kafka_radius_offsets"


    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      //LocationStrategies.PreferConsistent,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
      //ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets)
    )


    kafkaStream.foreachRDD((rdd, btime) => {
      if (!rdd.isEmpty()) {

        val lastBatchid = getLastestBatchid(topicName, groupId, hbaseTableName)
        val batchID = String.valueOf(btime.milliseconds)
        val rowRdd = rdd.map(x => parse(x.value()))

        val df = spark.createDataFrame(rowRdd, struct)

        df.write.mode(SaveMode.Overwrite).format("orc")
          .partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + batchID)
        val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + batchID + getTemplate + "/*.orc"))
        val filePartitions = new mutable.HashSet[String]
        for (i <- 0 until outFiles.length) {
          val nowPath = outFiles(i).getPath.toString
          filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + batchID, "").substring(1))
        }

        FileUtils.moveTempFiles(fileSystem, outputPath, batchID, getTemplate, filePartitions)
        val cost = System.currentTimeMillis() - btime.milliseconds
        val deltaMap = saveOffsets(topicName, groupId, rdd.asInstanceOf[HasOffsetRanges].offsetRanges, hbaseTableName, btime, cost)
        //df.show()
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

  def parse(msg: String) = {

    var row:Row = null

    try {
      val json = JSON.parseFull(msg)
      println(msg)
      json match {
        case Some(map: Map[String, String]) => {
          val bsid = map("BSID")
          val cdmaimsi = map("CDMAIMSI")
          val cellid = map("CELLID")
          val correlationid = map("CorrelationID")
          val duration = map("Duration")

          val eventtimestamp = map("EventTimestamp")
          val haserviceaddress = map("HAServiceAddress")
          val ipaddr = map("IPAddr")
          val inputoctets = map("InputOctets")
          val mdn = map("MDN")

          val nid = map("NID")
          val nettype = map("NetType")
          val outputoctets = map("OutputOctets")
          val recvtime = map("RecvTime")
          val sid = map("SID")

          val serviceoption = map("ServiceOption")
          val servicepcf = map("ServicePCF")
          val sessionid = map("SessionID")
          val status = map("Status")
          val terminatecause = map("TerminateCause")

          //val radiustime ="2018-12-28 13:07:05"// if (status == "Start") map("StartTime") else map("StopTime")


          val username = map("UserName")
          var beginSession = ""
          var radiustime = ""
          var sessioncontinue = ""
          if (status == "Start") {
            beginSession = map("BeginSession")
            radiustime = map("StartTime")
          }else{
            sessioncontinue = map("SessionContinue")
            radiustime = map("StopTime")
          }


          //val beginSession = map("BeginSession")

          // 2018-12-28 13:07:05
          val d = radiustime.substring(0, 10).replaceAll("-", "")
          val h = radiustime.substring(11, 13)
          val m5 = radiustime.substring(14, 15) + radiustime.substring(15, 16).toInt/5*5
          println(bsid + "," + cdmaimsi+ "," + cellid+ "," + correlationid+ "," + duration+ "," + eventtimestamp
            + "," + haserviceaddress+ "," + ipaddr+ "," + inputoctets+ "," + mdn+ "," + nid
            + "," + nettype+ "," + outputoctets+ "," + recvtime+ "," + sid
            + "," + serviceoption+ "," + servicepcf+ "," + sessioncontinue+ "," + sessionid
            + "," + status+ "," + radiustime+ "," + terminatecause+ "," + username
            + "," + d+ "," + h+ "," + m5)

          row = Row(
            bsid, cdmaimsi, cellid, correlationid, duration,
            eventtimestamp, haserviceaddress, ipaddr, inputoctets, mdn,
            nid, nettype, outputoctets, recvtime, sid,
            serviceoption, servicepcf,sessionid, status, terminatecause,
            username, beginSession, radiustime, sessioncontinue,
            d, h, m5
          )

        }
        case other => row = Row(
          "1", "-1", "-1", "-1", "-1",
          "-1", "-1", "-1", "-1", "-1",
          "-1", "-1", "-1", "-1", "-1",
          "-1", "-1", "-1", "-1", "-1",
          "-1", "-1", msg, "-1", "-1", "-1")
      }
      row
    } catch {
      case e: Exception => {
        Row(
          "0", "-1", "-1", "-1", "-1",
          "-1", "-1", "-1", "-1", "-1",
          "-1", "-1", "-1", "-1", "-1",
          "-1", "-1", "-1", "-1", "-1",
          "-1", "-1", msg, "-1", "-1", "-1")
      }
    }
  }


  val struct = StructType(Array(
    StructField("bsid", StringType),
    StructField("cdmaimsi", StringType),
    StructField("cellid", StringType),
    StructField("correlationid", StringType),
    StructField("duration", StringType),

    StructField("eventtimestamp", StringType),
    StructField("haserviceaddress", StringType),
    StructField("ipaddr", StringType),
    StructField("inputoctets", StringType),
    StructField("mdn", StringType),

    StructField("nid", StringType),
    StructField("nettype", StringType),
    StructField("outputoctets", StringType),
    StructField("recvtime", StringType),
    StructField("sid", StringType),

    StructField("serviceoption", StringType),
    StructField("servicepcf", StringType),
    StructField("sessionid", StringType),
    StructField("status", StringType),
    StructField("terminatecause", StringType),

    StructField("username", StringType),
    StructField("beginSession", StringType),
    StructField("radiustime", StringType),
    StructField("sessioncontinue", StringType),

    StructField("d", StringType),
    StructField("h", StringType),
    StructField("m5", StringType)
  ))

}