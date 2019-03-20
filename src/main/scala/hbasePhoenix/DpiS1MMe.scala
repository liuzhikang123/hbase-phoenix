package hbasePhoenix

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import Array._

/**
  * Created by zhoucw on 18-8-17 下午4:07.
  */
object DpiS1MMe {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("S6a").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.35,10.37.28.36,10.37.28.37:2181")
    val input = sc.getConf.get("spark.app.input", "/tmp/s1mme.csv.gz") //
    val htable = sc.getConf.get("spark.app.htable", "IOT_HB_MME")
    val output = sc.getConf.get("spark.app.output", "/hadoop/hb/data/mme")

    val rowRdd = sc.textFile(input).map(x => parse(x)).filter(_.length!=1)
    val df = sqlContext.createDataFrame(rowRdd, struct)

    df.repartition(20).write.format("orc").mode(SaveMode.Overwrite).save(output)

    val newDf = sqlContext.read.format("orc").load(output)
    newDf.show()

    newDf.selectExpr(
      "msisdn", "procedure_start_time", "procedure_end_time", "interface_type", "xdr_id",
      "imsi", "imei", "rat", "home_province", "procedure_type",
      "procedure_status", "reqcausecode", "failcausecode", "keyparam", "mme_ue_siap_id",
      "guti", "target_id", "user_ipv4", "user_ipv6", "machine_ip_add_type",
      "mme_sig_ip", "ran_ne_sig_ip", "mmeport", "enbport", "tai",
      "ecgi", "apn", "other_tac", "other_eci", "oldmmegroupid",
      "oldmmecode", "oldmtmsi", "mme_group_id", "mme_code", "m_tmsi",
      "tmsi", "mme_ue_s1ap_id", "enodeb_id", "eci", "epsbearernumber",
      "bearer_context"
    ).write.format("org.apache.phoenix.spark").
      mode(SaveMode.Overwrite).options( Map("table" -> htable,
      "zkUrl" -> zkUrl)).save()


  }
  val struct = StructType(Array(
    StructField("interface_type", StringType),
    StructField("xdr_id", StringType),
    StructField("imsi", StringType),
    StructField("imei", StringType),
    StructField("msisdn", StringType),

    StructField("rat", StringType),
    StructField("home_province", StringType),
    StructField("procedure_type", StringType),
    StructField("procedure_start_time", StringType),
    StructField("procedure_end_time", StringType),

    StructField("procedure_status", StringType),
    StructField("reqcausecode", StringType),
    StructField("failcausecode", StringType),
    StructField("keyparam", StringType),
    StructField("mme_ue_siap_id", StringType),

    StructField("guti", StringType),
    StructField("target_id", StringType),
    StructField("user_ipv4", StringType),
    StructField("user_ipv6", StringType),
    StructField("machine_ip_add_type", StringType),

    StructField("mme_sig_ip", StringType),
    StructField("ran_ne_sig_ip", StringType),
    StructField("mmeport", StringType),
    StructField("enbport", StringType),
    StructField("tai", StringType),

    StructField("ecgi", StringType),
    StructField("apn", StringType),
    StructField("other_tac", StringType),
    StructField("other_eci", StringType),
    StructField("oldmmegroupid", StringType),

    StructField("oldmmecode", StringType),
    StructField("oldmtmsi", StringType),
    StructField("mme_group_id", StringType),
    StructField("mme_code", StringType),
    StructField("m_tmsi", StringType),

    StructField("tmsi", StringType),
    StructField("mme_ue_s1ap_id", StringType),
    StructField("enodeb_id", StringType),
    StructField("eci", StringType),
    StructField("epsbearernumber", IntegerType),

    StructField("bearer_context", StringType)


  ))


  def parse(line: String) = {
    try {
      val fields = line.split("\\|")

      val interface_type = fields(0)
      val xdr_id = fields(1)
      val imsi = fields(2)
      val imei = fields(3)
      val msisdn = fields(4)

      val rat = fields(5)
      val home_province = fields(6)
      val procedure_type = fields(7)
      val procedure_start_time = fields(8)
      val procedure_end_time = fields(9)

      val procedure_status = fields(10)
      val reqcausecode = fields(11)
      val failcausecode = fields(12)
      val keyparam = fields(13)
      val mme_ue_siap_id = fields(14)

      val guti = fields(15)
      val target_id = fields(16)
      val user_ipv4 = fields(17)
      val user_ipv6 = fields(18)
      val machine_ip_add_type = fields(19)

      val mme_sig_ip = fields(20)
      val ran_ne_sig_ip = fields(21)
      val mmeport = fields(22)
      val enbport = fields(23)
      val tai = fields(24)

      val ecgi = fields(25)
      val apn = fields(26)
      val other_tac = fields(27)
      val other_eci = fields(28)
      val oldmmegroupid = fields(29)

      val oldmmecode = fields(30)
      val oldmtmsi = fields(31)
      val mme_group_id = fields(32)
      val mme_code = fields(33)
      val m_tmsi = fields(34)

      val tmsi = fields(35)
      val mme_ue_s1ap_id = fields(36)
      val enodeb_id = fields(37)

      val len = fields.length

      val eci = if(len>38) fields(38) else "-1"


      var epsbearernumber = 0
      var start = 39

      try{
        epsbearernumber = if(len>start) fields(start).toInt else 0
      }catch {
        case e:Exception => {
          epsbearernumber = 0
        }
      }

      var bearer_context = ""

      var index = 0

      for(i <- start until len){
        if(index % 9 == 0){
          bearer_context = bearer_context + "-" + fields(i)
        } else{
          bearer_context = bearer_context + "," + fields(i)
        }

        index = index + 1
        if(index == len){
          bearer_context = bearer_context.substring(1)
        }
      }



      val mdnReverse = msisdn.reverse

      Row(interface_type, xdr_id, imsi, imei, msisdn,
        rat, home_province, procedure_type, procedure_start_time, procedure_end_time,
        procedure_status, reqcausecode, failcausecode, keyparam, mme_ue_siap_id,
        guti, target_id, user_ipv4, user_ipv6, machine_ip_add_type,
        mme_sig_ip, ran_ne_sig_ip, mmeport, enbport, tai,
        ecgi, apn, other_tac, other_eci, oldmmegroupid,
        oldmmecode, oldmtmsi, mme_group_id, mme_code, m_tmsi,
        tmsi, mme_ue_s1ap_id, enodeb_id, eci, epsbearernumber,
        bearer_context)

    } catch {
      case e: Exception => {
        Row("0")
      }
    }
  }
}

