package hbasePhoenix.tmptotal

import java.sql.{DriverManager, PreparedStatement, Timestamp}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by liuzk on 18-11-29.
  * 13546
  * 十分钟、各省      S5S8 S6A COAP
  * 5个DPI指标，入到oracle的iotprodusernum表中
  */
object S5S8_S6a_Coap_SuccRates {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val appName = sc.getConf.get("spark.app.name", "dpiSuccRates_20181129")
    val ROMA_PROVINCE_csv = sc.getConf.get("spark.app.sgwSessions", "/user/slview/Dpi/ROMA_PROVINCE.csv")
    val S6A_ORIGIN_HOST = sc.getConf.get("spark.app.originHost", "/user/slview/Dpi/S6A_ORIGIN_HOST.csv")
    val S5S8Path = sc.getConf.get("spark.app.S5S8Path", "/user/slview/Dpi/S5S8_phoenix/")
    val S6APath = sc.getConf.get("spark.app.S6APath", "/user/slview/Dpi/S6a_phoenix/")
    val CoapPath = sc.getConf.get("spark.app.S1ucoapPath", "/user/slview/Dpi/S1ucoap_phoenix/")

    val thisTime = appName.substring(appName.lastIndexOf("_") + 1)
    val dataTime = thisTime.substring(0,11)

    val statime = dataTime.substring(0,4) + "/" + dataTime.substring(4,6) + "/" + dataTime.substring(6,8)+
      ' ' + dataTime.substring(8,10) + ":" +dataTime.substring(10,11) + '0'+":00"//2018/11/29 13:10:00
    val oraclrTime =new java.util.Date(statime)
    val jdbcDriver = "oracle.jdbc.driver.OracleDriver"
    val jdbcUrl = "jdbc:oracle:thin:@100.66.124.129:1521:dbnms"
    val jdbcUser = "epcslview"
    val jdbcPassword = "epc_slview129"



    import spark.implicits._
    val provDF = sc.textFile(ROMA_PROVINCE_csv).map(line=>line.split(",")).map(x=>(x(0),x(1))).toDF("ROMA_PROVINCE","PROVNAME")
    val originHostDF = sc.textFile(S6A_ORIGIN_HOST).map(line=>line.split(",")).map(x=>(x(0),x(1))).toDF("PROVNAME","origin")

    //////////////
    def insertByJDBC(result:Array[Row]) = {
      /*val deleteSQL = s"delete from iotprodusernum where DATATYPE='S5-C_CreateSession次数' and STA_TIME = to_date('${dataTime}','yyyymmdd')"
      var conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      var psmtdel: PreparedStatement = null
      psmtdel = conn.prepareStatement(deleteSQL)
      psmtdel.executeUpdate()
      conn.commit()
      psmtdel.close()*/

      //STA_TIME= to_date('2018-12-04 11:20:00','yyyy-MM-dd HH24:MI:SS')
      val insertSQL = "insert into iotprodusernum(DATATYPE,STA_TIME,PRODTYPE,USERNUM) values (?,?,?,?)"

      val dbConn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      dbConn.setAutoCommit(false)

      val pstmt = dbConn.prepareStatement(insertSQL)
      var i = 0
      try {
        for (r <- result) {
          val DATATYPE = r(0).toString
          val STA_TIME = statime//.toLong
          val PRODTYPE = r(2).toString
          val USERNUM = r(3).toString.toDouble
          //val USERNUM = Integer.parseInt(r(3).toString)

          pstmt.setString(1, DATATYPE)
          pstmt.setTimestamp(2, new Timestamp(oraclrTime.getTime()))
          pstmt.setString(3, PRODTYPE)
          pstmt.setDouble(4, USERNUM)

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

    try {
      //s5s8  承载建立成功率
      val S5S8DF = spark.read.format("orc").load(S5S8Path + dataTime + "*")
        .selectExpr("Procedure_Type", "Procedure_Status", "ROMA_PROVINCE")

      val S5S8DF_s = S5S8DF.filter("Procedure_Type=3 and Procedure_Status=0")
      val S5S8DF_succ = S5S8DF_s.groupBy("ROMA_PROVINCE").count.withColumnRenamed("count", "succ")
      val S5S8DF_succCount = S5S8DF_s.count().toDouble

      val S5S8DF_a = S5S8DF.filter("Procedure_Type=3")
      val S5S8DF_alls = S5S8DF_a.groupBy("ROMA_PROVINCE").count.withColumnRenamed("count", "alls")
      val S5S8DF_allsCount = S5S8DF_a.count().toDouble

      val S5S8RatesDF = S5S8DF_succ.join(S5S8DF_alls, Seq("ROMA_PROVINCE"))
        .selectExpr("'承载建立成功率' as DATATYPE", "ROMA_PROVINCE", "succ/alls*100 as rates")

      val result = S5S8RatesDF.join(provDF,Seq("ROMA_PROVINCE"),"inner")
        .selectExpr("DATATYPE",s"'${statime}' as STA_TIME","PROVNAME","rates").rdd.collect()

      val resultCountry = Array(Row("承载建立成功率",statime,"全国",S5S8DF_succCount*100/S5S8DF_allsCount))

      //insertByJDBC(result.union(resultCountry))
      insertByJDBC(result)
      insertByJDBC(resultCountry)

    }catch{
      case e: Exception => {
        e.printStackTrace()
        print("------------S5S8当前10分钟无数据--------")
      }
    }

    try{
      //  s6a HSS位置更新成功率
      val df = spark.read.format("orc").load(S6APath + dataTime + "*")
      val S6ADF = df.filter(df.col("ORIGIN_HOST").startsWith("mme"))
        .selectExpr("substr(ORIGIN_HOST,13,2) as origin","Procedure_Type","Procedure_Status")

      val S6ADF_s = S6ADF.filter("Procedure_Type=1 and Procedure_Status=0")
      val S6ADF_succ = S6ADF_s.groupBy("origin").count.withColumnRenamed("count", "succ")
      val S6ADF_succCount = S6ADF_s.count().toDouble

      val S6ADF_a = S6ADF.filter("Procedure_Type=1")
      val S6ADF_alls = S6ADF_a.groupBy("origin").count.withColumnRenamed("count", "alls")
      val S6ADF_allsCount = S6ADF_a.count().toDouble

      val S6ARatesDF = S6ADF_succ.join(S6ADF_alls,Seq("origin"))
        .selectExpr("'HSS位置更新成功率' as DATATYPE","origin","succ/alls*100 as rates")

      val result = S6ARatesDF.join(originHostDF,S6ARatesDF("origin") ===originHostDF("origin"),"inner")
        .selectExpr("DATATYPE",s"'${statime}' as STA_TIME","PROVNAME","rates").rdd.collect()

      val resultCountry = Array(Row("HSS位置更新成功率",statime,"全国",S6ADF_succCount*100/S6ADF_allsCount))

      //insertByJDBC(result.union(resultCountry))
      insertByJDBC(result)
      insertByJDBC(resultCountry)

    }catch{
      case e: Exception => {
        e.printStackTrace()
        print("------------S6A当前10分钟无数据-----------")
      }
    }


    try{

      val COAPDF = spark.read.format("orc").load(CoapPath + dataTime + "*")
        .selectExpr("ProcedureType","Status","Direction","ROMA_PROVINCE","FirstRequestTime","AckTime")

      //  coap  NB上行成功率
      val COAPDF_upS = COAPDF.filter("ProcedureType=1 and Direction=1 and (Status=0 or Status=2)")
      val COAPDF_upSucc = COAPDF_upS.groupBy("ROMA_PROVINCE").count.withColumnRenamed("count", "succ")
      val COAPDF_upSuccCount = COAPDF_upS.count().toDouble

      val COAPDF_upA = COAPDF.filter("ProcedureType=1 and Direction=1")
      val COAPDF_upAlls = COAPDF_upA.groupBy("ROMA_PROVINCE").count.withColumnRenamed("count", "alls")
      val COAPDF_upAllsCount = COAPDF_upA.count().toDouble

      val COAP_upRatesDF = COAPDF_upSucc.join(COAPDF_upAlls,Seq("ROMA_PROVINCE"))
        .selectExpr("'NB上行成功率' as DATATYPE","ROMA_PROVINCE","succ/alls*100 as rates")
      val COAP_upRatesDF_country = Array(Row("NB上行成功率",statime,"全国",COAPDF_upSuccCount*100/COAPDF_upAllsCount))
      COAP_upRatesDF_country

      //  coap  NB下行成功率
      val COAPDF_downS = COAPDF.filter("ProcedureType=1 and Direction=2 and (Status=0 or Status=2)")
      val COAPDF_downSucc = COAPDF_downS.groupBy("ROMA_PROVINCE").count.withColumnRenamed("count", "succ")
      val COAPDF_downSuccCount = COAPDF_downS.count().toDouble

      val COAPDF_downA = COAPDF.filter("ProcedureType=1 and Direction=2")
      val COAPDF_downAlls = COAPDF_downA.groupBy("ROMA_PROVINCE").count.withColumnRenamed("count", "alls")
      val COAPDF_downAllsCount = COAPDF_downA.count().toDouble

      val COAP_downRatesDF = COAPDF_downSucc.join(COAPDF_downAlls,Seq("ROMA_PROVINCE"))
        .selectExpr("'NB下行成功率' as DATATYPE","ROMA_PROVINCE","succ/alls*100 as rates")
      val COAP_downRatesDF_country = Array(Row("NB下行成功率",statime,"全国",COAPDF_downSuccCount*100/COAPDF_downAllsCount))


      //  coap  NB下行时延
      val COAPDF_timeDel = COAPDF_downA.selectExpr("ROMA_PROVINCE","(AckTime-FirstRequestTime) as timeDel")
        .groupBy("ROMA_PROVINCE").sum("timeDel").withColumnRenamed("sum(timeDel)","sumTimeDel")

      val COAPDF_timeDelCount = COAPDF_downA.selectExpr("sum(AckTime-FirstRequestTime) as timeDel")
        .map(x=>x.getDouble(0)).collect()(0)

      val COAP_downTimeDEL = COAPDF_downAlls.join(COAPDF_timeDel,Seq("ROMA_PROVINCE"))
        .selectExpr("'NB下行时延' as DATATYPE","ROMA_PROVINCE","sumTimeDel/alls as rates")
      val COAP_downTimeDEL_country = Array(Row("NB下行时延",statime,"全国",COAPDF_timeDelCount/COAPDF_downAllsCount))


      val ratesDF = COAP_upRatesDF.union(COAP_downRatesDF).union(COAP_downTimeDEL)
      val result = ratesDF.join(provDF,Seq("ROMA_PROVINCE"),"inner")
        .selectExpr("DATATYPE",s"'${statime}' as STA_TIME","PROVNAME","rates").rdd.collect()

      //insertByJDBC(result.union(COAP_upRatesDF_country).union(COAP_downRatesDF_country).union(COAP_downTimeDEL_country))
      insertByJDBC(result)
      insertByJDBC(COAP_upRatesDF_country)
      insertByJDBC(COAP_downRatesDF_country)
      insertByJDBC(COAP_downTimeDEL_country)
    }catch {
      case e: Exception => {
        e.printStackTrace()
        print("-------------COAP当前10分钟无数据-----------")
      }
    }


  }

  //spark-shell --master yarn-client  --num-executors 2 --executor-memory 3g --executor-cores 4

  //sqlContext.read.format("orc").load("/user/slview/Dpi/S5S8_phoenix/20180905*").repartition(100)

  //res5.selectExpr("RAT","Procedure_Status","SGW_IP_Add").write.format("orc").mode("overwrite").save("/tmp/tmp20180905")

  //sqlContext.read.format("orc").load("/tmp/tmp20180905").registerTempTable("aaa")

  //select SGW_IP_Add,count(*) from aaa where (RAT='8' or APN like '%ctnb%') and Procedure_Status='0' and SGW_IP_Add in('115.169.118.128','115.169.118.129','115.169.118.130','115.169.118.131','115.169.118.72','115.169.118.73','115.169.119.128','115.169.119.129','115.169.119.130','115.169.119.131','115.169.119.72','115.169.119.73','115.169.180.118','115.169.180.119','115.169.180.120','115.169.180.121','115.169.180.122','115.169.180.158','115.169.180.159','115.169.180.160','115.169.180.185','115.169.180.46','115.169.180.47','115.169.180.48','115.169.180.49','115.169.180.6','115.169.180.7','115.169.180.8','115.169.180.9','115.169.181.118','115.169.181.119','115.169.181.120','115.169.181.121','115.169.181.122','115.169.181.158','115.169.181.159','115.169.181.160','115.169.181.46','115.169.181.47','115.169.181.48','115.169.181.49','115.169.181.6','115.169.181.7','115.169.181.8','115.169.181.9','115.170.24.14','115.170.24.15','115.170.24.16','115.170.25.14','115.170.25.15','115.170.25.16','115.170.25.17') group by SGW_IP_Add
}
