package hbasePhoenix.tmptotal

import java.io.InputStream
import java.sql.{DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by liuzk on 18-9-26.
  * 13138 稽核
  */
object S5S8CreateSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val appName = sc.getConf.get("spark.app.name", "S5S8CreateSession_20180927")
    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val sgwSessions_csv = sc.getConf.get("spark.app.sgwSessions", "/user/slview/Dpi/S5S8_sgw_sessions.csv")
    val S5S8Path = sc.getConf.get("spark.app.S5S8Path", "/user/slview/Dpi/S5S8_phoenix/")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val statime = dataTime.substring(0,4) + "/" + dataTime.substring(4,6) + "/" + dataTime.substring(6,8)
    val oraclrTime =new java.util.Date(statime)
    //    val jdbcDriver = "oracle.jdbc.driver.OracleDriver"
    //    val jdbcUrl = "jdbc:oracle:thin:@100.66.124.129:1521:dbnms"
    //    val jdbcUser = "epcslview"
    //    val jdbcPassword = "epc_slview129"
    val postgprop = new Properties()
    val ipstream: InputStream = this.getClass().getResourceAsStream("/oracle.properties")
    postgprop.load(ipstream)
    val jdbcDriver = postgprop.getProperty("oracle.driver")
    val jdbcUrl = postgprop.getProperty("oracle.url")
    val jdbcUser = postgprop.getProperty("oracle.user")
    val jdbcPassword= postgprop.getProperty("oracle.password")


    import spark.implicits._
    val dfcsv = sc.textFile(sgwSessions_csv).map(line=>line.split(",")).map(x=>(x(0),x(1))).toDF("SGSNIPAddress","PROVNAME")
    val csvTable = "csvTable"
    dfcsv.createTempView(csvTable)

    val tableS5S8 = "tableS5S8"
    val df = spark.read.format("orc").load(S5S8Path + dataTime + "*")
    df.repartition(20).selectExpr("RAT","Procedure_Status","SGW_IP_Add","APN","Procedure_Type")
      .createTempView(tableS5S8)


    val sql =
      s"""
         |select 'S5-C_CreateSession次数' as DATATYPE, '${statime}' as STA_TIME,
         |        nvl(b.PROVNAME,"未知") as PRODTYPE, count(*) as USERNUM
         |from ${tableS5S8} a left join ${csvTable} b
         |on a.SGW_IP_Add = b.SGSNIPAddress
         |where a.APN like '%ctnb%' and a.Procedure_Status='0' and a.Procedure_Type='3'
         |group by b.PROVNAME
       """.stripMargin

    val result = spark.sql(sql).rdd.collect()

    insertByJDBC()

    def insertByJDBC() = {
      val deleteSQL = s"delete from iotprodusernum where DATATYPE='S5-C_CreateSession次数' and STA_TIME = to_date('${dataTime}','yyyymmdd')"
      var conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      var psmtdel: PreparedStatement = null
      psmtdel = conn.prepareStatement(deleteSQL)
      psmtdel.executeUpdate()
      conn.commit()
      psmtdel.close()

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
          val USERNUM = Integer.parseInt(r(3).toString)

          pstmt.setString(1, DATATYPE)
          pstmt.setDate(2, new java.sql.Date(oraclrTime.getTime()))
          pstmt.setString(3, PRODTYPE)
          pstmt.setInt(4, USERNUM)

          i += 1
          pstmt.addBatch()
          // 每1000条记录commit一次
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


  }
  //spark-shell --master yarn-client  --num-executors 2 --executor-memory 3g --executor-cores 4

  //sqlContext.read.format("orc").load("/user/slview/Dpi/S5S8_phoenix/20180905*").repartition(100)

  //res5.selectExpr("RAT","Procedure_Status","SGW_IP_Add").write.format("orc").mode("overwrite").save("/tmp/tmp20180905")

  //sqlContext.read.format("orc").load("/tmp/tmp20180905").registerTempTable("aaa")

  //select SGW_IP_Add,count(*) from aaa where (RAT='8' or APN like '%ctnb%') and Procedure_Status='0' and SGW_IP_Add in('115.169.118.128','115.169.118.129','115.169.118.130','115.169.118.131','115.169.118.72','115.169.118.73','115.169.119.128','115.169.119.129','115.169.119.130','115.169.119.131','115.169.119.72','115.169.119.73','115.169.180.118','115.169.180.119','115.169.180.120','115.169.180.121','115.169.180.122','115.169.180.158','115.169.180.159','115.169.180.160','115.169.180.185','115.169.180.46','115.169.180.47','115.169.180.48','115.169.180.49','115.169.180.6','115.169.180.7','115.169.180.8','115.169.180.9','115.169.181.118','115.169.181.119','115.169.181.120','115.169.181.121','115.169.181.122','115.169.181.158','115.169.181.159','115.169.181.160','115.169.181.46','115.169.181.47','115.169.181.48','115.169.181.49','115.169.181.6','115.169.181.7','115.169.181.8','115.169.181.9','115.170.24.14','115.170.24.15','115.170.24.16','115.170.25.14','115.170.25.15','115.170.25.16','115.170.25.17') group by SGW_IP_Add
}
