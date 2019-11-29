package hbasePhoenix.analysis

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by liuzk on 19-11-29.
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






  }

}
