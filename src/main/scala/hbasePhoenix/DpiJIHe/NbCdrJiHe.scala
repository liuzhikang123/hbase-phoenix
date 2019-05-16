package hbasePhoenix.DpiJIHe

import java.sql.{DriverManager, Timestamp}

import org.apache.spark.sql.{Row, SparkSession}
import java.text.SimpleDateFormat

/**
  * Created by liuzk on 19-4-16.
  *
  * 14418  nb话单与 DPI s5s8 coap数据稽核
  */
object NbCdrJiHe {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val appName = sc.getConf.get("spark.app.name", "NbCdrJiHe_20190416")
    val todayTime = sc.getConf.get("spark.app.todayTime", "20190417")
    val S5S8Path = sc.getConf.get("spark.app.S5S8Path", "/user/slview/Dpi/S5S8_phoenix/")
    val CoapPath = sc.getConf.get("spark.app.S1ucoapPath", "/user/slview/Dpi/S1ucoap_phoenix/")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val statime = dataTime.substring(0,4) + "/" + dataTime.substring(4,6) + "/" + dataTime.substring(6,8)
    val oraclrTime =new java.util.Date(statime)

    val thisTimeStamp = tranTimeToLong(dataTime)
    val todayTimeStamp = tranTimeToLong(todayTime)

    val jdbcDriver = "oracle.jdbc.driver.OracleDriver"
    val jdbcUrl = "jdbc:oracle:thin:@100.66.124.129:1521:dbnms"
    val jdbcUser = "epcslview"
    val jdbcPassword = "epc_slview129"



    val CoapDF = spark.read.format("orc").load(CoapPath + dataTime + "*", CoapPath + todayTime + "00*")
      .filter(s"APN='psma.edrx0.ctnb.mnc011.mcc460.gprs' and (PGWIP like '115.170.14.%' or PGWIP like '115.170.15.%') and StartTime>'${dataTime}' and StartTime<'${todayTime}' and (OutputOctets>0 or InputOctets>0)")
      .selectExpr("PGWIP", "MSISDN", "OutputOctets", "InputOctets")

    val tempTable_coap = "tempTable_coap"
    CoapDF.createTempView(tempTable_coap)
    spark.sqlContext.setConf("Spark.sql.inMemoryColummarStorage.compressed","true")
    spark.sqlContext.setConf("Spark.sql.inMemoryColummarStorage.BatchSize","10000")
    spark.sqlContext.cacheTable(tempTable_coap)
    val CoapDF_PGW1 = spark.sql(
      s"""
         |select '$dataTime' as STA_TIME, 'COAP' as CHECKTYPE, 'PGW1' as PGW, count(distinct MSISDN) as USERNUM,
         |       sum(OutputOctets) as UPFLOW, sum(InputOctets) as DOWNFLOW, (sum(OutputOctets)+sum(InputOctets)) as TOTALFLOW
         |from $tempTable_coap
         |where PGWIP like '115.170.14.%'
       """.stripMargin)

    val CoapDF_PGW2 = spark.sql(
      s"""
         |select '$dataTime' as STA_TIME, 'COAP' as CHECKTYPE, 'PGW2' as PGW, count(distinct MSISDN) as USERNUM,
         |       sum(OutputOctets) as UPFLOW, sum(InputOctets) as DOWNFLOW, (sum(OutputOctets)+sum(InputOctets)) as TOTALFLOW
         |from $tempTable_coap
         |where PGWIP like '115.170.15.%'
       """.stripMargin)

    val CoapDF_result = CoapDF_PGW1.union(CoapDF_PGW2).collect()
    spark.sqlContext.uncacheTable(tempTable_coap)



    val S5S8DF = spark.read.format("orc").load(S5S8Path + dataTime + "*", S5S8Path + todayTime + "00*")
      .filter(s"APN='ctnb.mnc011.mcc460.gprs' and (PGW_IP_Add like '115.170.14.%' or PGW_IP_Add like '115.170.15.%') and Procedure_Start_Time>'${thisTimeStamp}' and Procedure_Start_Time<'${todayTimeStamp}' and (Result_Code='16' or Result_Code='17' or Result_Code='18' or Result_Code='19')")
      .selectExpr("PGW_IP_Add","MSISDN", "Procedure_Type", "Procedure_Status").cache()

    val S5S8DF_PGW1 = S5S8DF.filter("PGW_IP_Add like '115.170.14.%'")
    val S5S8DF_PGW2 = S5S8DF.filter("PGW_IP_Add like '115.170.15.%'")
    // PGW1 PGW2 承载
    val S5S8DF_PGW1_CZ = S5S8DF_PGW1.filter("Procedure_Type=3 and Procedure_Status=0").count()//.toInt
    val S5S8DF_PGW2_CZ = S5S8DF_PGW2.filter("Procedure_Type=3 and Procedure_Status=0").count()
    // PGW1 PGW2 count(distinct mdn)
    val S5S8DF_PGW1_CM = S5S8DF_PGW1.dropDuplicates(Seq("MSISDN")).count()
    val S5S8DF_PGW2_CM = S5S8DF_PGW2.dropDuplicates(Seq("MSISDN")).count()

    val S5S8DF_result = Array(Row(dataTime, "S5C", "PGW1", S5S8DF_PGW1_CM, S5S8DF_PGW1_CZ, "-1", "-1"),
      Row(dataTime, "S5C", "PGW2", S5S8DF_PGW2_CM, S5S8DF_PGW2_CZ, "-1", "-1"))


    val DpiDF_result = CoapDF_result.union(S5S8DF_result)
    insertByJDBC(DpiDF_result)
    //insertByJDBC(CoapDF_result)
    //insertByJDBC(S5S8DF_result)

    //////////////Iot_checkdpiandcdr
    def insertByJDBC(result:Array[Row]) = {
//      val deleteSQL = s"delete from IOT_CHECKDPIANDCDR where CHECKTYPE like 'NB话单%' and STA_TIME = to_date('${dataTime}','yyyymmdd')"
//      var conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
//      var psmtdel: PreparedStatement = null
//      psmtdel = conn.prepareStatement(deleteSQL)
//      psmtdel.executeUpdate()
//      conn.commit()
//      psmtdel.close()

      //STA_TIME= to_date('2018-12-04 11:20:00','yyyy-MM-dd HH24:MI:SS')
      val insertSQL = "insert into IOT_CHECKDPIANDCDR(STA_TIME,CHECKTYPE,PGW,USERNUM,UPFLOW,DOWNFLOW,TOTALFLOW) values (?,?,?,?,?,?,?)"

      val dbConn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      dbConn.setAutoCommit(false)

      val pstmt = dbConn.prepareStatement(insertSQL)
      var i = 0
      try {
        for (r <- result) {
          val STA_TIME = r(0).toString
          val CHECKTYPE = r(1).toString
          val PGW = r(2).toString
          val USERNUM = Integer.parseInt(r(3).toString)
          val UPFLOW = r(4).toString.toDouble
          val DOWNFLOW = r(5).toString.toDouble
          val TOTALFLOW = r(6).toString.toDouble

          pstmt.setDate(1, new java.sql.Date(oraclrTime.getTime()))
          pstmt.setString(2, CHECKTYPE)
          pstmt.setString(3, PGW)
          pstmt.setInt(4, USERNUM)
          pstmt.setDouble(5, UPFLOW)
          pstmt.setDouble(6, DOWNFLOW)
          pstmt.setDouble(7, TOTALFLOW)

          i += 1
          pstmt.addBatch()
          if (i % 1000 == 0) {
            pstmt.executeBatch
          }
        }

        pstmt.executeBatch
        dbConn.commit()
        pstmt.close()

      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }
    }
    //////////////

  }
  //时间转时间戳
  //https://blog.csdn.net/weixin_40163498/article/details/80759726
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyyMMdd")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }
}