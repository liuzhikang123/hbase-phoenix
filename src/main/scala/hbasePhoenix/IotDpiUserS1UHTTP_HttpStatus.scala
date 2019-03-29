package hbasePhoenix

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by liuzk on 19-03-22.
  * http数据过滤 status="302" or ""
  */
object IotDpiUserS1UHTTP_HttpStatus {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val input = sc.getConf.get("spark.app.input", "/user/slview/Dpi/S1uhttp") //
    val htable = sc.getConf.get("spark.app.htable", "IOT_DPI_USER_S1U_HTTP_STATUS_20190322")
    val output = sc.getConf.get("spark.app.output", "/user/slview/Dpi/S1uhttp_phoenix/HttpStatus/")
    val repartitionNum = sc.getConf.get("spark.app.repartitionNum", "1").toInt
    val appName = sc.getConf.get("spark.app.name")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    import sqlContext.implicits._
    val df = sc.textFile(input + "/"+ dataTime +"/").map(x=>x.split("\\|",-1)).filter(_.length==100)
      .map(x=>(x(2),x(20),x(21),x(5),x(69),x(66),x(75)))
      .toDF("MSISDN","StartTime","EndTime","DestinationIP","Host","UserAgent","HttpStatus")

    df.filter(df.col("HttpStatus")==="302" or df.col("HttpStatus")==="")
      .coalesce(repartitionNum).write.format("orc").mode(SaveMode.Overwrite)
      .save(output + dataTime)

    val newDf = sqlContext.read.format("orc").load(output + dataTime)
    val resultDF = newDf.selectExpr("MSISDN","StartTime","EndTime","DestinationIP","Host","UserAgent","HttpStatus")

    resultDF.write.format("org.apache.phoenix.spark").
      mode(SaveMode.Overwrite).options( Map("table" -> htable,
      "zkUrl" -> zkUrl)).save()


  }
}
