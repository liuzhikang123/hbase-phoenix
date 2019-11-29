package hbasePhoenix.analysis

import java.sql.PreparedStatement

import com.zyuc.dpi.java.utils.DbUtils_tidbonline4g
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * Created by liuzk on 19-11-29.
  * 16081
  * 从S5S8中，每小时取去重sgw_add_ip，定期写入TIDB 4g库
  */
object IotDpiCheckSgwip {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    // val spark = SparkSession.builder().appName("Spark2SavePhoenixDF").master("local[*]").getOrCreate()
    //  val spark = SparkSession.builder().enableHiveSupport().appName("name_123-201805221210").master("local[3]").getOrCreate()
    //val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val inputPath = sc.getConf.get("spark.app.inputPath", "/user/slview/Dpi/S5S8_phoenix/")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/user/slview/Dpi/tmp/IOT_DPI_CHECKSGWIP/")
    val tidbTable = sc.getConf.get("spark.app.tidbTable", "IOT_DPI_CHECKSGWIP")
    val repartitionNum = sc.getConf.get("spark.app.repartitionNum", "10").toInt
    val appName = sc.getConf.get("spark.app.name")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    spark.read.format("orc").load(inputPath + dataTime + "*")
      .selectExpr("SGW_IP_Add").dropDuplicates(Seq("SGW_IP_Add"))
      .write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "tmp")

    val tmptable = "tmptable"
    val datatable = "datatable"
    spark.read.format("orc").load(outputPath + "tmp").createTempView(tmptable)
    spark.read.format("orc").load(outputPath + "data").createTempView(datatable)

    val newSgwAddIpDF = spark.sql(
      s"""
         |select a.SGW_IP_Add sgw_add_ip, b.SGW_IP_Add
         |from ${tmptable} a
         |left join ${datatable} b on(a.SGW_IP_Add=b.SGW_IP_Add)
       """.stripMargin).filter("SGW_IP_Add is null").selectExpr("sgw_add_ip").dropDuplicates("sgw_add_ip")

    //先追加到基础库里
    newSgwAddIpDF.write.format("orc").mode(SaveMode.Append).save(outputPath + "data")

    insert2Tidb(tidbTable, newSgwAddIpDF)


    def insert2Tidb(tidbTable:String, df:DataFrame) = {
      val result = df.collect()

      // 将结果写入到tidb
      var dbConn = DbUtils_tidbonline4g.getDBConnection
      var pstmt: PreparedStatement = null

      // 执行insert操作
      dbConn.setAutoCommit(false)
      val sql =
        s"""
           |insert into ${tidbTable}(sta_time, sgw_add_ip) values (?,?)
       """.stripMargin

      pstmt = dbConn.prepareStatement(sql)
      var i = 0
      for(r<-result){
        val sgw_add_ip = r(0).toString

        pstmt.setString(1, dataTime+"0000")
        pstmt.setString(2, sgw_add_ip)


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
    }






  }

}
