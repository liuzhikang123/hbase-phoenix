package hbasePhoenix.tmptotal

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by liuzk on 18-11-14.
  */
object S1UCOAP_total1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val appName = sc.getConf.get("spark.app.name", "S1UCOAP_20181113")
    val sgwSessions_csv = sc.getConf.get("spark.app.sgwSessions", "/user/slview/Dpi/S5S8_sgw_sessions.csv")
    val S1UCOAPPath = sc.getConf.get("spark.app.S1UCOAPPath", "/user/slview/Dpi/S1ucoap_phoenix/")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    //-----------------------------------
    import spark.implicits._
    val dfcsv = sc.textFile(sgwSessions_csv).map(line=>line.split(",")).map(x=>(x(0),x(1))).toDF("SGSNIPAddress","PROVNAME")
    val csvTable = "csvTable"
    dfcsv.createTempView(csvTable)

    val mawu_NB = sc.textFile("/user/slview/Dpi/tmp/813542_81_mawu_NB.txt").map(line=>line.split(",")).map(x=>x(0)).toDF("MDN")
    val mawu_NBTable = "mawu_NB"
    mawu_NB.createTempView(mawu_NBTable)

    val xinjie_NB = sc.textFile("/user/slview/Dpi/tmp/813540_80_xinjie_NB.txt").map(line=>line.split(",")).map(x=>x(0)).toDF("MDN")
    val xinjie_NBTable = "xinjie_NB"
    xinjie_NB.createTempView(xinjie_NBTable)
    //-----------------------------------

    val tableCOAP = "tableCOAP"
    val df = spark.read.format("orc").load(S1UCOAPPath +dataTime+ "*/")
    df.repartition(20).selectExpr(s"${dataTime} as whichDay ", "MSISDN", "Home_Province", "ProcedureType","Direction",
      "Status","AppServerIP","SGWIP").filter("AppServerIP='117.60.157.137'")
      .createTempView(tableCOAP)

    spark.sql(
      s"""
         |select whichDay, MSISDN, PROVNAME, ProcedureType, Direction, Status
         |from ${tableCOAP} a
         |left join ${csvTable} b on(a.SGWIP = b.SGSNIPAddress)
         |where a.MSISDN in (select distinct MDN from ${mawu_NBTable})
       """.stripMargin).createTempView("MAWu_table")
    ///////
    // and (status='0' or status='2')
    val upcons = spark.sql(
      s"""
         |select whichDay,MSISDN,PROVNAME,count(*) as upcons
         |from MAWu_table
         |where ProcedureType='1' and Direction='1'
         |group by whichDay,MSISDN, PROVNAME
       """.stripMargin)
    val upnons = spark.sql(
      s"""
         |select whichDay,MSISDN,PROVNAME,count(*) as upnons
         |from MAWu_table
         |where ProcedureType='2' and Direction='1'
         |group by whichDay,MSISDN, PROVNAME
       """.stripMargin)
    val downcons = spark.sql(
      s"""
         |select whichDay,MSISDN,PROVNAME,count(*) as downcons
         |from MAWu_table
         |where ProcedureType='1' and Direction='2'
         |group by whichDay,MSISDN, PROVNAME
       """.stripMargin)
    val downconSucc = spark.sql(
      s"""
         |select whichDay,MSISDN,PROVNAME,count(*) as downconSucc
         |from MAWu_table
         |where ProcedureType='1' and Direction='2' and (status='0' or status='2')
         |group by whichDay,MSISDN, PROVNAME
       """.stripMargin)
    val downnons = spark.sql(
      s"""
         |select whichDay,MSISDN,PROVNAME,count(*) as downnons
         |from MAWu_table
         |where ProcedureType='2' and Direction='2'
         |group by whichDay,MSISDN, PROVNAME
       """.stripMargin)

    upcons.join(upnons,Seq("whichDay","MSISDN","PROVNAME"),"inner")
      .join(downcons,Seq("whichDay","MSISDN","PROVNAME"),"inner")
      .join(downconSucc,Seq("whichDay","MSISDN","PROVNAME"),"inner")
      .join(downnons,Seq("whichDay","MSISDN","PROVNAME"),"inner").write.mode(SaveMode.Append).csv("/user/slview/DPi/tmp/mawu_result")

    ////////////////////////
    spark.sql(
      s"""
         |select whichDay, MSISDN, PROVNAME, ProcedureType, Direction, Status
         |from ${tableCOAP} a
         |left join ${csvTable} b on(a.SGWIP = b.SGSNIPAddress)
         |where a.MSISDN in (select distinct MDN from ${xinjie_NBTable})
       """.stripMargin).createTempView("XINJIE_table")
    // and (status='0' or status='2')
    val upcons1 = spark.sql(
      s"""
         |select whichDay,MSISDN,PROVNAME,count(*) as upcons
         |from XINJIE_table
         |where ProcedureType='1' and Direction='1'
         |group by whichDay,MSISDN, PROVNAME
       """.stripMargin)
    val upnons1 = spark.sql(
      s"""
         |select whichDay,MSISDN,PROVNAME,count(*) as upnons
         |from XINJIE_table
         |where ProcedureType='2' and Direction='1'
         |group by whichDay,MSISDN, PROVNAME
       """.stripMargin)
    val downcons1 = spark.sql(
      s"""
         |select whichDay,MSISDN,PROVNAME,count(*) as downcons
         |from XINJIE_table
         |where ProcedureType='1' and Direction='2'
         |group by whichDay,MSISDN, PROVNAME
       """.stripMargin)
    val downconSucc1 = spark.sql(
      s"""
         |select whichDay,MSISDN,PROVNAME,count(*) as downconSucc
         |from XINJIE_table
         |where ProcedureType='1' and Direction='2' and (status='0' or status='2')
         |group by whichDay,MSISDN, PROVNAME
       """.stripMargin)
    val downnons1 = spark.sql(
      s"""
         |select whichDay,MSISDN,PROVNAME,count(*) as downnons
         |from XINJIE_table
         |where ProcedureType='2' and Direction='2'
         |group by whichDay,MSISDN, PROVNAME
       """.stripMargin)

    upcons1.join(upnons1,Seq("whichDay","MSISDN","PROVNAME"),"inner")
      .join(downcons1,Seq("whichDay","MSISDN","PROVNAME"),"inner")
      .join(downconSucc1,Seq("whichDay","MSISDN","PROVNAME"),"inner")
      .join(downnons1,Seq("whichDay","MSISDN","PROVNAME"),"inner").write.mode(SaveMode.Append).csv("/user/slview/DPi/tmp/xinjie_result")


    upcons.show(30)
    spark.sql("select * from MAWu_table limit 30").show()

  }
}
