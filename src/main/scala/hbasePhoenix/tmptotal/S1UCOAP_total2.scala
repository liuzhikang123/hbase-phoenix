package hbasePhoenix.tmptotal

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by liuzk on 19-1-7.
  *
  * 签约ctnb且只签约DefaultNBiot业务的卡中有访问除117.60.157.137地址外的号卡列表
  * 统计7天的就行了，coap的目的ip
  */
object S1UCOAP_total2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val htable = sc.getConf.get("spark.app.htable", "IOT_DPI_SIGN_S5S8_20180830")
    val df = spark.read.format("org.apache.phoenix.spark")
      .options( Map("table" -> "IOT_DPI_USER_S1U_COAP_20190106",
        "zkUrl" -> zkUrl)).load()
    val df1 = df.selectExpr("APN","MSISDN","DestinationIP")

    val df2 = spark.read.format("org.apache.phoenix.spark")
      .options( Map("table" -> "IOT_DPI_USER_S1U_COAP_20190105",
        "zkUrl" -> zkUrl)).load()
      .selectExpr("APN","MSISDN","DestinationIP")

    val df3 = spark.read.format("org.apache.phoenix.spark")
      .options( Map("table" -> "IOT_DPI_USER_S1U_COAP_20190104",
        "zkUrl" -> zkUrl)).load()
      .selectExpr("APN","MSISDN","DestinationIP")

    val df4 = spark.read.format("org.apache.phoenix.spark")
      .options( Map("table" -> "IOT_DPI_USER_S1U_COAP_20190103",
        "zkUrl" -> zkUrl)).load()
      .selectExpr("APN","MSISDN","DestinationIP")

    val df5 = spark.read.format("org.apache.phoenix.spark")
      .options( Map("table" -> "IOT_DPI_USER_S1U_COAP_20190102",
        "zkUrl" -> zkUrl)).load()
      .selectExpr("APN","MSISDN","DestinationIP")

    val df6 = spark.read.format("org.apache.phoenix.spark")
      .options( Map("table" -> "IOT_DPI_USER_S1U_COAP_20190101",
        "zkUrl" -> zkUrl)).load()
      .selectExpr("APN","MSISDN","DestinationIP")

    val df7 = spark.read.format("org.apache.phoenix.spark")
      .options( Map("table" -> "IOT_DPI_USER_S1U_COAP_20181231",
        "zkUrl" -> zkUrl)).load()
      .selectExpr("APN","MSISDN","DestinationIP")

    val df7Days = df1.union(df2).union(df3).union(df4).union(df5).union(df6).union(df7)
    df7Days.selectExpr("APN","MSISDN","DestinationIP").coalesce(200).write.format("orc").mode(SaveMode.Overwrite).save("/tmp/tmpCOAP7Days")

    val tableCOAP7Days = "tableCOAP7Days"
    spark.read.format("orc").load("/tmp/tmpCOAP7Days")
      .filter("APN like '%ctnb%' and DestinationIP!='117.60.157.137'").coalesce(20).createTempView(tableCOAP7Days)

    val tableA = "tableA"
    spark.sql(
      s"""
         |select MSISDN,concat_ws(',',collect_list(DestinationIP)) as DestinationIP_value from ${tableCOAP7Days} group by MSISDN
       """.stripMargin).createTempView(tableA)

    //df.repartition(20).write.format("orc").mode(SaveMode.Overwrite).save("/tmp/tmpCOAPtable")
    import spark.implicits._
    val mdnTable = "mdnTable"
    sc.textFile("/tmp/result.txt").toDF("mdn").createTempView(mdnTable)

    spark.sql(
      s"""
         |select mdn,DestinationIP_value
         |from ${mdnTable} m inner join ${tableA} t
         |on m.mdn=t.MSISDN
       """.stripMargin).coalesce(1).rdd.saveAsTextFile("/tmp/tmpCOAP20190107")
    /*spark.sql(
      s"""
         |select DestinationIP
         |from ${tableCOAP7Days}
         |where MSISDN in ('115.170.14.82',
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
       """.stripMargin).rdd.saveAsTextFile("/tmp/tmpCOAP20190107")*/

  }
}
