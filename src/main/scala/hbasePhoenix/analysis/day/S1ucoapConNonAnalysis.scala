package hbasePhoenix.analysis.day

import java.sql.PreparedStatement

import com.zyuc.dpi.java.utils.DbUtils
import com.zyuc.dpi.utils.CommonUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by liuzk on 20-04-20.
  * 企业级 分接入省市的 NB
  * 上行CON消息请求数   上行CON消息成功数   上行NON消息数   上行CON成功率
  * 入到TIDB里面    --需求黄志慧
  */
object S1ucoapConNonAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val appName = sc.getConf.get("spark.app.name", "S1ucoapConNonAnalysis")
    val crmPath = sc.getConf.get("spark.app.crmPath", "/user/slview/CRM/data/d=20180510/")
    val roma_province_csv = sc.getConf.get("spark.app.sgwSessions", "/user/slview/Dpi/ROMA_PROVINCE.csv")
    val bsInfoData = sc.getConf.get("spark.app.bsInfoData", "/user/slview/IotBSInfo/data")
    val s1ucoaPath = sc.getConf.get("spark.app.s1ucoaPath", "/user/slview/Dpi/S1ucoap_phoenix/")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    //  crm
    val crmTable = "crmTable"
    spark.read.format("parquet").load(crmPath).selectExpr("mdn", "custid")
      .createTempView(crmTable)

//    //  接入省份信息
//    import spark.implicits._
//    val provTable = "provTable"
//    sc.textFile(roma_province_csv).map(line=>line.split(",")).map(x=>(x(0),x(1))).toDF("ROMA_PROVINCE","PROVNAME")
//      .createTempView(provTable)

    //  基站信息
    // enbid|provId|provName|cityId|cityName|             zhLabel|           userLabel|vendorId|           vndorName|
    val bsTable = "bsTable"
    spark.read.format("orc").load(bsInfoData).createTempView(bsTable)

    //  coap
    val s1ucoapTable = "s1ucoapTable"
    spark.read.format("orc").load(s1ucoaPath + dataTime+"*")
      .filter("APN like '%ctnb%'")
      .selectExpr("MSISDN", "ProcedureType", "Direction", "Status", "enb_id")
      .createTempView(s1ucoapTable)


    val unionTable = "unionTable"
    spark.sql(
      s"""
         |select c.custid, b.provName, b.cityName, a.ProcedureType, a.Direction, a.Status
         |from ${s1ucoapTable}  a
         |left join ${bsTable}  b on(a.enb_id=b.enbid)
         |left join ${crmTable} c on(a.MSISDN=c.mdn)
       """.stripMargin)
      .createTempView(unionTable)

    val df = spark.sql(
      s"""
         |select custid, provName, cityName,
         |sum(case when ProcedureType='1' then 1 else 0 end) upConcnt,
         |sum(case when ProcedureType='1' and (Status=0 or Status=2) then 1 else 0 end) upConSucccnt,
         |sum(case when ProcedureType='2' then 1 else 0 end) upNoncnt
         |from ${unionTable}
         |where Direction='1'
         |group by custid, provName, cityName
       """.stripMargin)

//    df.show()
//    df.printSchema()
//    df.write.format("orc").mode(SaveMode.Overwrite).save("/user/slview/Dpi/tmp/tmp20200420")

    val resultCollect = df
      .selectExpr(s"'${dataTime}' datatime", "custid", "provName", "cityName", "upConcnt", "upConSucccnt", "upNoncnt", "round(upConSucccnt*100/upConcnt,2) upConRate")
      .rdd.collect()



    // 将结果写入到tidb
      var dbConn = DbUtils.getDBConnection
      dbConn.setAutoCommit(false)
      val sql =
        s"""
           |insert into iot_dpi_ana_s1ucoapconnon_prov
           |(datatime, custid, provName, cityName, upConcnt, upConSucccnt, upNoncnt, upConRate)
           |values (?,?,?,?,?,?,?,?)
       """.stripMargin

      val pstmt = dbConn.prepareStatement(sql)

      var i = 0
      for(r<-resultCollect){
        val datatime = coverNull(r(0))
        val custid = coverNull(r(1))
        val provName = coverNull(r(2))
        val cityName = coverNull(r(3))
        val upConcnt = coverNull(r(4)).toInt
        val upConSucccnt = coverNull(r(5)).toInt
        val upNoncnt = coverNull(r(6)).toInt
        val upConRate = coverNull(r(7)).toString.toDouble

        pstmt.setString(1, datatime)
        pstmt.setString(2, custid)
        pstmt.setString(3, provName)
        pstmt.setString(4, cityName)
        pstmt.setInt(5, upConcnt)
        pstmt.setInt(6, upConSucccnt)
        pstmt.setInt(7, upNoncnt)
        pstmt.setDouble(8, upConRate)

        i += 1
        pstmt.addBatch()
        if (i % 1000 == 0) {
          pstmt.executeBatch
          dbConn.commit()
        }
      }
      pstmt.executeBatch
      dbConn.commit()
      pstmt.close()
      dbConn.close()

      // 更新断点时间
      CommonUtils.updateBreakTable("iot_dpi_ana_s1ucoapconnon_prov", dataTime)

    def coverNull(value:Any): String ={
      var v="-1"
      try{
        v = value.toString
        v
      } catch {
        case e: Exception => {
          //e.printStackTrace()
        }
          v
      }
      v
    }



  }
}
//CREATE TABLE `iot_dpi_ana_s1ucoapconnon_prov` (
//`datatime` varchar(8) DEFAULT NULL,
//`custid` varchar(100) DEFAULT NULL,
//`provName` varchar(100) DEFAULT NULL,
//`cityName` varchar(100) DEFAULT NULL,
//`upConcnt` int(10) DEFAULT NULL,
//`upConSucccnt` int(10) DEFAULT NULL,
//`upNoncnt` int(10) DEFAULT NULL,
//`upConRate` decimal(5,2) DEFAULT NULL
//) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;