package hbasePhoenix

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import Array._

/**
  * Created by zhoucw on 18-8-17 下午4:07.
  */
object DpiHttp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("S6a").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.35,10.37.28.36,10.37.28.37:2181")
    val input = sc.getConf.get("spark.app.input", "/tmp/http.gz") //
    val htable = sc.getConf.get("spark.app.htable", "IOT_HB_HTTP")
    val output = sc.getConf.get("spark.app.output", "/hadoop/hb/data/http")

    val rowRdd = sc.textFile(input).map(x => parse(x))
    val df = sqlContext.createDataFrame(rowRdd, struct)

    df.repartition(10).write.format("orc").mode(SaveMode.Overwrite).save(output)

    val newDf = sqlContext.read.format("orc").load(output)


    newDf.selectExpr(
      "msisdn", "starttime", "endtime", "imsi", "imei",
      "apn", "destinationip", "destinationport", "sourceip", "sourceport",
      "sgw_ip", "mme_ip", "pgw_ip", "ecgi_temp", "tai_temp",
      "visitedplmnid", "rattype", "protocolid", "servicetype_new", "duration",
      "inputoctets", "outputoctets", "inputpacket", "outputpacket", "pdn_connectionid",
      "bearerid", "bearerqos", "recordclosecause", "useragent", "destinationurl_new",
      "domainname", "host", "contentlen", "contenttype", "iflink",
      "refer", "httpaction", "httpstatus", "respdelay", "behaviortarget",
      "syntime", "synacktime", "acktime", "actiontime", "firstpackettime",
      "pageopentime", "lastpacktime", "lastpacktime_tmp", "pagevolume" )
      .write.format("org.apache.phoenix.spark").
      mode(SaveMode.Overwrite).options( Map("table" -> htable,
      "zkUrl" -> zkUrl)).save()


  }
  val struct = StructType(Array(
    StructField("imsi", StringType),
    StructField("msisdn", StringType),
    StructField("imei", StringType),
    StructField("apn", StringType),
    StructField("destinationip", StringType),

    StructField("destinationport", StringType),
    StructField("sourceip", StringType),
    StructField("sourceport", StringType),
    StructField("sgw_ip", StringType),
    StructField("mme_ip", StringType),

    StructField("pgw_ip", StringType),
    StructField("ecgi_temp", StringType),
    StructField("tai_temp", StringType),
    StructField("visitedplmnid", StringType),
    StructField("rattype", StringType),

    StructField("protocolid", StringType),
    StructField("servicetype_new", StringType),
    StructField("starttime", StringType),
    StructField("endtime", StringType),
    StructField("duration", StringType),

    StructField("inputoctets", StringType),
    StructField("outputoctets", StringType),
    StructField("inputpacket", StringType),
    StructField("outputpacket", StringType),
    StructField("pdn_connectionid", StringType),

    StructField("bearerid", StringType),
    StructField("bearerqos", StringType),
    StructField("recordclosecause", StringType),
    StructField("useragent", StringType),
    StructField("destinationurl_new", StringType),

    StructField("domainname", StringType),
    StructField("host", StringType),
    StructField("contentlen", StringType),
    StructField("contenttype", StringType),
    StructField("iflink", StringType),

    StructField("refer", StringType),
    StructField("httpaction", StringType),
    StructField("httpstatus", StringType),
    StructField("respdelay", StringType),
    StructField("behaviortarget", StringType),

    StructField("syntime", StringType),
    StructField("synacktime", StringType),
    StructField("acktime", StringType),
    StructField("actiontime", StringType),
    StructField("firstpackettime", StringType),

    StructField("pageopentime", StringType),
    StructField("lastpacktime", StringType),
    StructField("lastpacktime_tmp", StringType),
    StructField("pagevolume", StringType)

  ))


  def parse(line: String) = {
    try {
      val fields = line.split("\\|")

      val imsi = fields(0)
      val msisdn = fields(1)
      val imei = fields(2)
      val apn = fields(3)
      val destinationip = fields(4)

      val destinationport = fields(5)
      val sourceip = fields(6)
      val sourceport = fields(7)
      val sgw_ip = fields(8)
      val mme_ip = fields(9)

      val pgw_ip = fields(10)
      val ecgi_temp = fields(11)
      val tai_temp = fields(12)
      val visitedplmnid = fields(13)
      val rattype = fields(14)

      val protocolid = fields(15)
      val servicetype_new = fields(16)
      val starttime = fields(17)
      val endtime = fields(18)
      val duration = fields(19)

      val inputoctets = fields(20)
      val outputoctets = fields(21)
      val inputpacket = fields(22)
      val outputpacket = fields(23)
      val pdn_connectionid = fields(24)

      val bearerid = fields(25)
      val bearerqos = fields(26)
      val recordclosecause = fields(27)
      val useragent = fields(28)
      val destinationurl_new = fields(29)

      val domainname = fields(30)
      val host = fields(31)
      val contentlen = fields(32)
      val contenttype = fields(33)
      val iflink = fields(34)

      val refer = fields(35)
      val httpaction = fields(36)
      val httpstatus = fields(37)
      val respdelay = fields(38)
      val behaviortarget = fields(39)

      val len = fields.length

      val syntime = if(len > 40) fields(40) else "-1"
      val synacktime = if(len > 41) fields(41) else "-1"
      val acktime = if(len > 42) fields(42) else "-1"
      val actiontime = if(len > 43) fields(43) else "-1"
      val firstpackettime = if(len > 44) fields(44) else "-1"

      val pageopentime = if(len > 45) fields(45) else "-1"
      val lastpacktime = if(len > 46) fields(46) else "-1"
      val lastpacktime_tmp = if(len > 47) fields(47) else "-1"
      val pagevolume = if(len > 48) fields(48) else "-1"

      val mdnReverse = msisdn.reverse

      Row(imsi, msisdn, imei, apn, destinationip,
        destinationport, sourceip, sourceport, sgw_ip, mme_ip,
        pgw_ip, ecgi_temp, tai_temp, visitedplmnid, rattype,
        protocolid, servicetype_new, starttime, endtime, duration,
        inputoctets, outputoctets, inputpacket, outputpacket, pdn_connectionid,
        bearerid, bearerqos, recordclosecause, useragent, destinationurl_new,
        domainname, host, contentlen, contenttype, iflink,
        refer, httpaction, httpstatus, respdelay, behaviortarget,
        syntime, synacktime, acktime, actiontime, firstpackettime,
        pageopentime, lastpacktime, lastpacktime_tmp, pagevolume)

    } catch {
      case e: Exception => {
        e.printStackTrace()
        Row("0")
      }
    }
  }
}

