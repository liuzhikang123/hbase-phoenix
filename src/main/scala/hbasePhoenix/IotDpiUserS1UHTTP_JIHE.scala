package hbasePhoenix

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by liuzk on 19-03-13.
  * DPI拨测稽查
  */
object IotDpiUserS1UHTTP_JIHE {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val input = sc.getConf.get("spark.app.input", "/user/slview/Dpi/S1uhttp") //
    val htable = sc.getConf.get("spark.app.htable", "IOT_DPI_USER_S1U_HTTP_JIHE_201902")
    val output = sc.getConf.get("spark.app.output", "/user/slview/Dpi/S1uhttp_phoenix/jihe/")
    val repartitionNum = sc.getConf.get("spark.app.repartitionNum", "1").toInt
    val appName = sc.getConf.get("spark.app.name")//2020031017
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    val rdd1 = sc.textFile(input + "/"+dataTime+"*/").filter(x=>x.contains("861410307300000"))
    val rdd2 = sc.textFile(input + "/"+dataTime+"*/").filter(x=>x.contains("861410309800480"))
    val rowRdd = rdd1.union(rdd2).map(x => parse(x)).filter(_.length!=1)
    //val rowRdd = sc.textFile(input + "/"+dataTime+"*/").map(x => parse(x)).filter(_.length!=1)
    val df = sqlContext.createDataFrame(rowRdd, struct)

    df.filter("length(MSISDN)>0")
        .dropDuplicates(Seq(//有重复的http文件，先暂时做成这样去重。。。
          "StartTime",
          "EndTime",
          "MSISDN",
          "IMSI",
          "APN",

          "PGWIP",
          "DestinationURL",
          "UserIP",
          "UserPort",
          "SourceIP",
          "SourcePort"))
      .write.format("orc").mode(SaveMode.Overwrite).save(output + dataTime)

    val newDf = sqlContext.read.format("orc").load(output + dataTime)

    val resultDF = newDf.selectExpr(
      "StartTime",
      "EndTime",
      "MSISDN",
      "IMSI",
      "APN",

      "PGWIP",
      "DestinationURL",
      "UserIP",
      "UserPort",
      "SourceIP",
      "SourcePort"
    )//.filter("MSISDN='861410307300000' or MSISDN='861410309800480'")

    resultDF.write.format("org.apache.phoenix.spark").
      mode(SaveMode.Overwrite).options( Map("table" -> htable,
      "zkUrl" -> zkUrl)).save()

    resultDF.repartition(1).write.format("com.databricks.spark.csv")
      .option("header","true").option("delimiter","|")
      .mode(SaveMode.Overwrite).save(output + dataTime + "_csv")

  }
  val struct = StructType(Array(
    StructField("IMSI", StringType),
    StructField("MSISDN", StringType),
    StructField("APN", StringType),
    StructField("SourceIP", StringType),
    StructField("SourcePort", StringType),

    StructField("PGWIP", StringType),
    StructField("StartTime", StringType),
    StructField("EndTime", StringType),
    StructField("UserIP", StringType),
    StructField("UserPort", StringType),

    StructField("DestinationURL", StringType)

  ))

  def parse(line: String) = {
    try {
      val fields = line.split("\\|",-1)

      val IMSI = fields(1)
      val MSISDN = fields(2)
      val APN = fields(4)
      val SourceIP = fields(5)
      val SourcePort = fields(6)

      val PGWIP = fields(11)
      val StartTime = fields(20)
      val EndTime = fields(21)
      val UserIP = fields(7)
      val UserPort = fields(8)

      val DestinationURL = fields(67)
      val Host = fields(69)

      Row(IMSI,MSISDN,APN, SourceIP,SourcePort,
        PGWIP,StartTime, EndTime, UserIP, UserPort,
        Host + DestinationURL)

    } catch {
      case e: Exception => {
        Row("0")
      }
    }
  }

}
