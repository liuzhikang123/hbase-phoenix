package hbasePhoenix.tmptotal

import java.sql.PreparedStatement

import com.zyuc.dpi.java.utils.DbUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by liuzk on 18-10-26.
  * 980000078950
  * 20190308 于老师 临时三天统计
  */
object S1UCOAP_total4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val appName = sc.getConf.get("spark.app.name", "S1UCOAP_20181027")
    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val CRMpath = sc.getConf.get("spark.app.crmpath", "/user/slview/CRM/data/d=20180510/")
    val S5S8path = sc.getConf.get("spark.app.S5S8path", "/user/slview/Dpi/S5S8_phoenix/")
    val S1ucoapath = sc.getConf.get("spark.app.S1UCOAPPath", "/user/slview/Dpi/S1ucoap_phoenix/")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    // CRM  filter("custid='980000078950'")
    val crmTable = "crmTable"
    spark.read.format("orc").load(CRMpath)
      .filter("custid='980000078950'").selectExpr("mdn", "custid as cust_id")
      .createOrReplaceTempView(crmTable)
    val crmCacheTable = "crmCacheTable"
    spark.sql(
      s"""
         |cache table ${crmCacheTable}
         |as
         |select * from ${crmTable}
       """.stripMargin)


    val table_S5S8 = "table_S5S8"
    val df = spark.read.format("orc").load(S5S8path + dataTime+"*")
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
         |select MSISDN,
         |sum(case when Procedure_Type='2' then 1 else 0 end) all_cnt,
         |sum(case when Procedure_Status!='0' then 1 else 0 end) fail_cnt
         |from ${table_S5S8}
         |where Procedure_Type='2' and MSISDN in(select mdn from ${crmTable})
         |group by MSISDN
       """.stripMargin).createTempView(df_s5s82)

    //  coap
    val table_S1ucoap = "table_S1ucoap"
    val df1 = spark.read.format("orc").load(S1ucoapath + dataTime+"*")
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

    insert2Tidb("s5s8_coap", "/user/slview/Dpi/tmp/s5s8_coap")

    def insert2Tidb(table:String, datapath:String) = {
      val df = spark.read.format("csv").load(datapath)
      val result = df.collect()

      // 将结果写入到tidb
      var dbConn = DbUtils.getDBConnection

      // 先删除结果
      //数据保留6天吧，你写个删除，我也会定期删除
      val deleteSQL =
      s"""
         |delete from ${table} where datatime=?
       """.stripMargin
      var pstmt: PreparedStatement = null
      pstmt = dbConn.prepareStatement(deleteSQL)
      pstmt.setString(1, dataTime)
      pstmt.executeUpdate()
      pstmt.close()

      // 执行insert操作
      dbConn.setAutoCommit(false)
      val sql =
        s"""
           |insert into ${table}
           |(datatime, cust_id, mdn, alls, fails, mdn1, all_cnt, fail_cnt,
           |mdn2, upcnt, up_fail_cnt, down_cnt, down_fail_cnt,
           |mdn3, up1_cnt, up1_fail_cnt, down1_cnt, down1_fail_cnt)
           |values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
       """.stripMargin

      pstmt = dbConn.prepareStatement(sql)
      var i = 0
      for(r<-result){
        val cust_id = coverNull(r(0))
        val mdn = coverNull(r(1))
        val alls = coverNull(r(2))
        val fails = coverNull(r(3))
        val mdn1 = coverNull(r(4))
        val all_cnt = coverNull(r(5))

        val fail_cnt = coverNull(r(6))
        val mdn2 = coverNull(r(7))
        val upcnt = coverNull(r(8))
        val up_fail_cnt = coverNull(r(9))
        val down_cnt = coverNull(r(10))
        val down_fail_cnt = coverNull(r(11))

        val mdn3 = coverNull(r(12))
        val up1_cnt = coverNull(r(13))
        val up1_fail_cnt = coverNull(r(14))
        val down1_cnt = coverNull(r(15))
        val down1_fail_cnt = coverNull(r(16))


        /*pstmt.setString(1, dataTime)
        pstmt.setString(2, cust_id)
        pstmt.setString(3, mdn)
        pstmt.setInt(4, alls.toString.toInt)
        pstmt.setInt(5, fails.toString.toInt)
        pstmt.setString(6, mdn1)
        pstmt.setInt(7, all_cnt.toString.toInt)

        pstmt.setInt(8, succ_cnt.toString.toInt)
        pstmt.setString(9, mdn2)
        pstmt.setInt(10, upcnt.toString.toInt)
        pstmt.setInt(11, up_fail_cnt.toString.toInt)
        pstmt.setInt(12, down_cnt.toString.toInt)
        pstmt.setInt(13, down_fail_cnt.toString.toInt)

        pstmt.setString(14, mdn3)
        pstmt.setInt(15, up1_cnt.toString.toInt)
        pstmt.setInt(16, up1_fail_cnt.toString.toInt)
        pstmt.setInt(17, down1_cnt.toString.toInt)
        pstmt.setInt(18, down1_fail_cnt.toString.toInt)*/
        pstmt.setString(1, dataTime)
        pstmt.setString(2, cust_id)
        pstmt.setString(3, mdn)
        pstmt.setString(4, alls)
        pstmt.setString(5, fails)
        pstmt.setString(6, mdn1)
        pstmt.setString(7, all_cnt)

        pstmt.setString(8, fail_cnt)
        pstmt.setString(9, mdn2)
        pstmt.setString(10, upcnt)
        pstmt.setString(11, up_fail_cnt)
        pstmt.setString(12, down_cnt)
        pstmt.setString(13, down_fail_cnt)

        pstmt.setString(14, mdn3)
        pstmt.setString(15, up1_cnt)
        pstmt.setString(16, up1_fail_cnt)
        pstmt.setString(17, down1_cnt)
        pstmt.setString(18, down1_fail_cnt)


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

    def coverNull(value:Any): String ={
      var v="-1"
      try{
        v = value.toString
        v
      } catch {
        case e: Exception => {
          //e.printStackTrace()
        }
          v
      }
      v
    }

  }
}
