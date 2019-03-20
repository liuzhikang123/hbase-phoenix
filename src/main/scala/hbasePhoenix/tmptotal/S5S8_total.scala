package hbasePhoenix.tmptotal

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by liuzk on 18-9-6.
  */
object S5S8_total {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val htable = sc.getConf.get("spark.app.htable", "IOT_DPI_SIGN_S5S8_20180830")
    val df = spark.read.format("org.apache.phoenix.spark")
      .options( Map("table" -> htable,
        "zkUrl" -> zkUrl)).load()
    df.show(20)
    //df.repartition(20).write.format("orc").mode(SaveMode.Overwrite).save("/tmp/tmpS5S8table")

    val tableS5S8 = "tableS5S8"
    df.filter("RAT='8' and Procedure_Status='0'").createTempView(tableS5S8)
    spark.sql(
      s"""
         |select SGW_IP_Add,count(*)
         |from ${tableS5S8}
         |where RAT='8' and Procedure_Status='0' and SGW_IP_Add in ('115.170.14.82',
         |'115.170.15.84',
         |'115.170.14.57',
         |'115.170.15.82',
         |'115.170.15.90',
         |'115.170.14.56',
         |'115.170.15.56',
         |'115.170.15.57',
         |'115.170.14.91',
         |'115.170.15.83',
         |'115.170.14.83',
         |'115.170.14.84',
         |'115.170.15.91',
         |'115.170.14.90')
         |group by SGW_IP_Add
       """.stripMargin).show()//.write.format("orc").mode("overwrite").save("/tmp/tmp20180906")

  }
}
