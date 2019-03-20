package hbasePhoenix.tmptotal

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by liuzk on 18-10-26.
  */
object S1UCOAP_total {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val appName = sc.getConf.get("spark.app.name", "S1UCOAP_20181027")
    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val sgwSessions_csv = sc.getConf.get("spark.app.sgwSessions", "/user/slview/Dpi/S5S8_sgw_sessions.csv")
    val S1UCOAPPath = sc.getConf.get("spark.app.S1UCOAPPath", "/user/slview/.Trash/181028140000/user/slview/Dpi/S1ucoap_phoenix/")

    import spark.implicits._
    /*val dfcsv = sc.textFile(sgwSessions_csv).map(line => line.split(",")).map(x => (x(0), x(1))).toDF("SGSNIPAddress", "PROVNAME")
    val csvTable = "csvTable"
    dfcsv.createTempView(csvTable)*/

    val tableCOAP = "tableCOAP"
    val df = spark.read.format("orc").load(S1UCOAPPath + "*/*")
    df.repartition(20).selectExpr("Status", "SGWIP", "Direction", "ProcedureType")
      .filter("SGWIP='115.169.132.160' or SGWIP='115.169.132.161' or " +
        "SGWIP='115.169.132.162' or SGWIP='115.169.132.163' or " +
        "SGWIP='115.169.132.164'")
      .createTempView(tableCOAP)

    spark.sql(
      s"""
         |select count(*) from ${tableCOAP} where ProcedureType='1' and Direction='2'
       """.stripMargin).write.format("orc").mode(SaveMode.Overwrite).save("/user/slview/Dpi/tmp/coap1")

    spark.sql(
      s"""
         |select count(*) from ${tableCOAP} where ProcedureType='1' and Direction='2' and (status='0' or status='2')
       """.stripMargin).write.format("orc").mode(SaveMode.Overwrite).save("/user/slview/Dpi/tmp/coap2")

  }
}
