package hbasePhoenix

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 18-8-17 下午4:07.
  */
object DpiS6aETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("S6a").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.35,10.37.28.36,10.37.28.37:2181")
    val input = sc.getConf.get("spark.app.input", "/tmp/s6a.gz")
    val htable = sc.getConf.get("spark.app.htable", "IOT_HB_S6")

    val rowRdd = sc.textFile(input).map(x=>parse(x)).filter(_.length!=0)

    val df = sqlContext.createDataFrame(rowRdd, struct)

    df.selectExpr("mdnReverse", "msisdn", "procedure_start_time", "procedure_end_time", "interface_type", "xdr_id",
      "rat", "imsi", "imei", "procedure_type", "procedure_status",
      "cause", "user_ip", "mme_address", "hss_address", "mmeport",
      "hssport", "originrealm", "destinationrealm", "originhost", "destinationhost",
      "applicationid", "subscriberstatus", "accessrestrictiondata", "user_ipv4", "user_ipv6",
      "layer1id", "layer2id", "layer3id", "layer4id", "layer5id",
      "layer6id", "machine_ip_add_type", "default_apn", "ue_ambr_ul", "ue_ambr_dl",
      "home_province", "visit_mcc", "visit_mnc", "probeid")
      .write.format("org.apache.phoenix.spark").
      mode(SaveMode.Overwrite).options( Map("table" -> htable,
      "zkUrl" -> zkUrl)).save()
  }

  val struct = StructType(Array(
    StructField("mdnReverse", StringType),
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
    StructField("cause", StringType),
    StructField("user_ip", StringType),
    StructField("mme_address", StringType),
    StructField("hss_address", StringType),
    StructField("mmeport", StringType),
    StructField("hssport", StringType),
    StructField("originrealm", StringType),
    StructField("destinationrealm", StringType),
    StructField("originhost", StringType),
    StructField("destinationhost", StringType),
    StructField("applicationid", StringType),
    StructField("subscriberstatus", StringType),
    StructField("accessrestrictiondata", StringType),
    StructField("user_ipv4", StringType),
    StructField("user_ipv6", StringType),
    StructField("layer1id", StringType),
    StructField("layer2id", StringType),
    StructField("layer3id", StringType),
    StructField("layer4id", StringType),
    StructField("layer5id", StringType),
    StructField("layer6id", StringType),
    StructField("machine_ip_add_type", StringType),
    StructField("default_apn", StringType),
    StructField("ue_ambr_ul", StringType),
    StructField("ue_ambr_dl", StringType),
    StructField("home_province", StringType),
    StructField("visit_mcc", StringType),
    StructField("visit_mnc", StringType),
    StructField("probeid", StringType)
  ))

  def parse(line:String) = {
    try{
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

      val cause = fields(10)
      val user_ip = fields(11)
      val mme_address = fields(12)
      val hss_address = fields(13)
      val mmeport = fields(14)

      val hssport = fields(15)
      val originrealm = fields(16)
      val destinationrealm = fields(17)
      val originhost = fields(18)
      val destinationhost = fields(19)

      val applicationid = fields(20)
      val subscriberstatus = fields(21)
      val accessrestrictiondata = fields(22)
      val user_ipv4 = fields(23)
      val user_ipv6 = fields(24)

      val layer1id = fields(25)
      val layer2id = fields(26)
      val layer3id = fields(27)
      val layer4id = fields(28)
      val layer5id = fields(29)

      val layer6id = fields(30)
      val machine_ip_add_type = fields(31)
      val default_apn = fields(32)
      val ue_ambr_ul = fields(33)
      val ue_ambr_dl = fields(34)

      val home_province = fields(35)
      val visit_mcc = fields(36)
      val visit_mnc = fields(37)
      val probeid = fields(38)

      val mdnReverse = msisdn.reverse

      Row(mdnReverse, interface_type, xdr_id, rat, imsi, imei,
        msisdn, procedure_type, procedure_start_time, procedure_end_time, procedure_status,
        cause, user_ip, mme_address, hss_address, mmeport,
        hssport, originrealm, destinationrealm, originhost, destinationhost,
        applicationid, subscriberstatus, accessrestrictiondata, user_ipv4, user_ipv6,
        layer1id, layer2id, layer3id, layer4id, layer5id,
        layer6id, machine_ip_add_type, default_apn, ue_ambr_ul, ue_ambr_dl,
        home_province, visit_mcc, visit_mnc, probeid )

    }catch {
      case e:Exception => {
        Row("0")
      }
    }
  }
}

