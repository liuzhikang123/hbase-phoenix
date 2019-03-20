package hbasePhoenix

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import Array._

/**
  * Created by zhoucw on 18-8-17 下午4:07.
  */
object DpiS5S8 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("S6a").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.35,10.37.28.36,10.37.28.37:2181")
    val input = sc.getConf.get("spark.app.input", "/tmp/S5S8.csv.gz") //
    val htable = sc.getConf.get("spark.app.htable", "IOT_HB_S5S8")
    val output = sc.getConf.get("spark.app.output", "/hadoop/hb/data/s5s8")

    val rowRdd = sc.textFile(input).map(x => parse(x))
    val df = sqlContext.createDataFrame(rowRdd, struct)
    df.selectExpr("bearerinfo").show(false)

    df.repartition(100).write.format("orc").mode(SaveMode.Overwrite).save(output)

    val newDf = sqlContext.read.format("orc").load(output)

    newDf.selectExpr("msisdn", "procedure_start_time", "procedure_end_time", " interface_type", "xdr_id",
      "rat", "imsi", "imei", "procedure_type", "procedure_status",
      "failure_cause", "user_ip", "sgw_ip_add", "pgw_ip_add", "sgw_port",
      "pgw_port", "sgwcontrolteid", "pgwcontrolteid", "indicationflags", "ulilength",
      "uli", "epsbearernumber", "bearer1id", "bearer1type", "bearer1qci",
      "bearer1status", "bearer1sgw_gtp_teid", "bearer1pgw_gtp_teid", "sai_cgi_ecgi", "rai_tai",
      "user_ipv4", "user_ipv6", "layer1id", "layer2id", "layer3id",
      "layer4id", "layer5id", "layer6id", "apn", "apn_ambr_ul",
      "apn_ambr_dl", "machine_ip_add_type", "bearerinfo", "home_province", "probeid" )
      .write.format("org.apache.phoenix.spark").
      mode(SaveMode.Overwrite).options( Map("table" -> htable,
      "zkUrl" -> zkUrl)).save()


  }
  val struct = StructType(Array(
    StructField("interface_type", StringType),
    StructField("xdr_id", StringType),
    StructField("rat", StringType),
    StructField("imsi", StringType),
    StructField("imei", StringType),

    StructField("msisdn", StringType),
    StructField("procedure_type", StringType),
    StructField("procedure_start_time", StringType),
    StructField("procedure_end_time", StringType),
    StructField("procedure_status", StringType),

    StructField("failure_cause", StringType),
    StructField("user_ip", StringType),
    StructField("sgw_ip_add", StringType),
    StructField("pgw_ip_add", StringType),
    StructField("sgw_port", StringType),

    StructField("pgw_port", StringType),
    StructField("sgwcontrolteid", StringType),
    StructField("pgwcontrolteid", StringType),
    StructField("indicationflags", StringType),
    StructField("ulilength", IntegerType),

    StructField("uli", StringType),
    StructField("epsbearernumber", StringType),
    StructField("bearer1id", StringType),
    StructField("bearer1type", StringType),
    StructField("bearer1qci", StringType),

    StructField("bearer1status", StringType),
    StructField("bearer1sgw_gtp_teid", StringType),
    StructField("bearer1pgw_gtp_teid", StringType),
    StructField("sai_cgi_ecgi", StringType),
    StructField("rai_tai", StringType),

    StructField("user_ipv4", StringType),
    StructField("user_ipv6", StringType),
    StructField("layer1id", StringType),
    StructField("layer2id", StringType),
    StructField("layer3id", StringType),

    StructField("layer4id", StringType),
    StructField("layer5id", StringType),
    StructField("layer6id", StringType),
    StructField("apn", StringType),
    StructField("apn_ambr_ul", StringType),

    StructField("apn_ambr_dl", StringType),
    StructField("machine_ip_add_type", StringType),
    StructField("bearerinfo", StringType),
    StructField("home_province", StringType),
    StructField("probeid", StringType)
  ))


  def parse(line: String) = {
    try {
      val fields = line.split("\\|")

      val interface_type = fields(0)
      val xdr_id = fields(1)
      val rat = fields(2)
      val imsi = fields(3)
      val imei = fields(4)

      val msisdn = fields(5)
      val procedure_type = fields(6)
      val procedure_start_time = fields(7)
      val procedure_end_time = fields(8)
      val procedure_status = fields(9)

      val failure_cause = fields(10)
      val user_ip = fields(11)
      val sgw_ip_add = fields(12)
      val pgw_ip_add = fields(13)
      val sgw_port = fields(14)

      val pgw_port = fields(15)
      val sgwcontrolteid = fields(16)
      val pgwcontrolteid = fields(17)
      val indicationflags = fields(18)

      var ulilength = 0
      try{
        ulilength = fields(19).toInt
      }catch {
        case e:Exception => {
          ulilength = 0
          //e.printStackTrace()
        }
      }

      val uli = fields(20)
      val epsbearernumber = fields(21)
      val bearer1id = fields(22)
      val bearer1type = fields(23)
      val bearer1qci = fields(24)

      val bearer1status = fields(25)
      val bearer1sgw_gtp_teid = fields(26)
      val bearer1pgw_gtp_teid = fields(27)
      val sai_cgi_ecgi = fields(28)
      val rai_tai = fields(29)

      val user_ipv4 = fields(30)
      val user_ipv6 = fields(31)
      val layer1id = fields(32)
      val layer2id = fields(33)
      val layer3id = fields(34)

      val layer4id = fields(35)
      val layer5id = fields(36)
      val layer6id = fields(37)
      val apn = fields(38)
      val apn_ambr_ul = fields(39)

      val apn_ambr_dl = fields(40)
      val machine_ip_add_type = fields(41)

      var index = 42
      var bearerinfo = ""


      try{
        for(i<- 0 until ulilength){
          val newarr = new Array[String](6)
          copy(fields, index, newarr, 0, 6 )
          bearerinfo = bearerinfo + "," + newarr.mkString("-")
          index = index + 6

        }
      }catch {
        case e:Exception=>{
          println("length:" + fields.length)
          println("index:" + index)
          println("ulilength:" + ulilength)
        }
      }


      val home_province = fields(index)
      val probeid = fields(index + 1)

      val mdnReverse = msisdn.reverse

      Row(interface_type, xdr_id, rat, imsi, imei,
        msisdn, procedure_type, procedure_start_time, procedure_end_time, procedure_status,
        failure_cause, user_ip, sgw_ip_add, pgw_ip_add, sgw_port,
        pgw_port, sgwcontrolteid, pgwcontrolteid, indicationflags, ulilength,
        uli, epsbearernumber, bearer1id, bearer1type, bearer1qci,
        bearer1status, bearer1sgw_gtp_teid, bearer1pgw_gtp_teid, sai_cgi_ecgi, rai_tai,
        user_ipv4, user_ipv6, layer1id, layer2id, layer3id,
        layer4id, layer5id, layer6id, apn, apn_ambr_ul,
        apn_ambr_dl, machine_ip_add_type, bearerinfo, home_province, probeid)

    } catch {
      case e: Exception => {
        e.printStackTrace()
        Row("0")
      }
    }
  }
}

