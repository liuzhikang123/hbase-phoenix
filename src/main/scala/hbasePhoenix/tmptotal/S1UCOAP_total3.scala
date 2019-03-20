package hbasePhoenix.tmptotal

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by liuzk on 18-10-26.
  * 980000083303
  */
object S1UCOAP_total3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val appName = sc.getConf.get("spark.app.name", "S1UCOAP_20181027")
    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val CRMpath = sc.getConf.get("spark.app.crmpath", "/user/slview/CRM/data/d=20180510/")
    val S5S8path = sc.getConf.get("spark.app.S5S8path", "/user/slview/Dpi/S5S8_phoenix/")
    val S1ucoapath = sc.getConf.get("spark.app.S1UCOAPPath", "/user/slview/Dpi/S1ucoap_phoenix/")

    // CRM  filter("custid='980000083303'")
    val crmTable = "crmTable"
    spark.read.format("orc").load(CRMpath)
      .filter("custid='980000083303'").selectExpr("mdn", "custid as cust_id")
      .createOrReplaceTempView(crmTable)
    val crmCacheTable = "crmCacheTable"
    spark.sql(
      s"""
         |cache table ${crmCacheTable}
         |as
         |select * from ${crmTable}
       """.stripMargin)


    val table_S5S8 = "table_S5S8"
    val df = spark.read.format("orc").load(S5S8path + "/20190228*",S5S8path + "/20190301*")
    df.selectExpr("MSISDN", "Procedure_Type", "Procedure_Status")
      .repartition(20).createTempView(table_S5S8)

    //  S5S8
    val df_s5s81 = "df_s5s81"
      spark.sql(
      s"""
         |select MSISDN, count(*) as alls,
         |sum(case when Procedure_Status!='0' then 1 else 0 end) fails
         |from ${table_S5S8}
         |where Procedure_Type='3' and MSISDN in(select mdn from ${crmTable})
         |group by MSISDN
       """.stripMargin).createTempView(df_s5s81)

    val df_s5s82 = "df_s5s82"
      spark.sql(
      s"""
         |select MSISDN, count(*) as all_cnt,
         |sum(case when Procedure_Status='0' then 1 else 0 end) fail_cnt
         |from ${table_S5S8}
         |where Procedure_Type='2' and MSISDN in(select mdn from ${crmTable})
         |group by MSISDN
       """.stripMargin).createTempView(df_s5s82)

    //  coap
    val table_S1ucoap = "table_S1ucoap"
    val df1 = spark.read.format("orc").load(S1ucoapath + "/20190228*",S1ucoapath + "/20190301*")
    df1.selectExpr("MSISDN", "ProcedureType", "Direction", "Status")
      .repartition(20).createTempView(table_S1ucoap)

    val df_coap1 = "df_coap1"
      spark.sql(
      s"""
         |select MSISDN,
         |sum(case when Direction='1' then 1 else 0 end) upcnt,
         |sum(case when Direction='1' and Status!='0' and Status!='2' then 1 else 0 end) up_fail_cnt,
         |sum(case when Direction='2' then 1 else 0 end) down_cnt,
         |sum(case when Direction='2' and Status!='0' and Status!='2' then 1 else 0 end) down_fail_cnt
         |from ${table_S1ucoap}
         |where ProcedureType='1' and MSISDN in(select mdn from ${crmTable})
         |group by MSISDN
       """.stripMargin).createTempView(df_coap1)

    val df_coap2 = "df_coap2"
      spark.sql(
      s"""
         |select MSISDN,
         |sum(case when Direction='1' then 1 else 0 end) up1_cnt,
         |sum(case when Direction='1' and Status!='0' and Status!='2'  then 1 else 0 end) up1_fail_cnt,
         |sum(case when Direction='2' then 1 else 0 end) down1_cnt,
         |sum(case when Direction='2' and Status!='0' and Status!='2'  then 1 else 0 end) down1_fail_cnt
         |from ${table_S1ucoap}
         |where ProcedureType='2' and MSISDN in(select mdn from ${crmTable})
         |group by MSISDN
       """.stripMargin).createTempView(df_coap2)


    val saveoptions = Map("header" -> "true", "delimiter" -> ",", "path" -> "/user/slview/Dpi/tmp/s5s8_coap")
    spark.sql(
      s"""
         |select '980000083303' as cust_id,
         |        a.MSISDN, alls, fails, b.MSISDN as MSISDN1, all_cnt, fail_cnt,
         |        c.MSISDN as MSISDN2, upcnt, up_fail_cnt, down_cnt, down_fail_cnt,
         |        d.MSISDN as MSISDN3, up1_cnt, up1_fail_cnt, down1_cnt, down1_fail_cnt
         |from df_s5s81 a
         |full join df_s5s82 b on(a.MSISDN=b.MSISDN)
         |full join df_coap1 c on(a.MSISDN=c.MSISDN)
         |full join df_coap2 d on(a.MSISDN=d.MSISDN)
       """.stripMargin).repartition(1)
      .write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveoptions).save()

  }
}
