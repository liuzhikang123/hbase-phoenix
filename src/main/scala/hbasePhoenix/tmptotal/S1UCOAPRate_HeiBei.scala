package hbasePhoenix.tmptotal

import com.zyuc.dpi.java.utils.DbUtils
import com.zyuc.dpi.utils.CommonUtils
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by liuzk on 18-11-26.
  * 13423 nb企业coap成功率及失败原因
  * 河北演示视图
  */
object S1UCOAPRate_HeiBei {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val appName = sc.getConf.get("spark.app.name", "S1UCOAPRate_20181125")
    val inputpath = sc.getConf.get("spark.app.inputpath", "/user/slview/Dpi/S1ucoap_phoenix/")
    val crmpath = sc.getConf.get("spark.app.crmpath", "/user/slview/CRM/data/d=20180510/")

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    val crmTable = "crmTable"
    spark.read.format("orc").load(crmpath).selectExpr("mdn", "custid as cust_id").createOrReplaceTempView(crmTable)
    val crmCacheTable = "crmCacheTable"
    spark.sql(
      s"""
         |cache table ${crmCacheTable}
         |as
         |select * from ${crmTable} where cust_id in(
         |'500000890944',
         |'500000909279',
         |'500000953374',
         |'980000021519',
         |'980000024650',
         |'980000035497',
         |'980000037305',
         |'980000040006',
         |'980000066370',
         |'980000070254',
         |'980000076519',
         |'980000078950',
         |'980000084277',
         |'980000635244',
         |'980000645501',
         |'980000671505',
         |'980000678825',
         |'980000679873')
       """.stripMargin)

    val coapDFTable = "coapDFTable"
    val COAPDF = spark.read.format("orc").load(inputpath + dataTime + "*")
      .filter("APN like '%ctnb%'")
      .selectExpr("ProcedureType","Status","Direction","MSISDN","FirstRequestTime","AckTime")
    COAPDF.createOrReplaceTempView(coapDFTable)

    //  coap  NB上行成功率
    val coapUp = spark.sql(
      s"""
         |select cust_id, 'NB上行成功率' as gather_type, round((succ_cnt*100/cnt),2) gather_value
         |from
         |(select cust_id, count(*) cnt,
         |        sum(case when Status=0 or Status=2 then 1 else 0 end) succ_cnt
         |from ${coapDFTable} c, ${crmCacheTable} m
         |where c.MSISDN = m.mdn and ProcedureType=1 and Direction=1
         |group by cust_id
         |) a
       """.stripMargin)

    //  coap  NB下行成功率
    val coapDown = spark.sql(
      s"""
         |select cust_id, 'NB下行成功率' as gather_type, round((succ_cnt*100/cnt),2) gather_value
         |from
         |(select cust_id, count(*) cnt,
         |        sum(case when Status=0 or Status=2 then 1 else 0 end) succ_cnt
         |from ${coapDFTable} c, ${crmCacheTable} m
         |where c.MSISDN = m.mdn and ProcedureType=1 and Direction=2
         |group by cust_id
         |) a
       """.stripMargin)

    //  coap  NB下行时延
    val coapDown_timeDel = spark.sql(
      s"""
         |select cust_id, 'NB下行时延' as gather_type,
         |        round(avg(AckTime-FirstRequestTime),2) as gather_value
         |from ${coapDFTable} c, ${crmCacheTable} m
         |where c.MSISDN = m.mdn and ProcedureType=1 and Direction=2
         |group by cust_id
       """.stripMargin)

    val result = coapUp.union(coapDown).union(coapDown_timeDel)
      .selectExpr("cust_id", "gather_type", "gather_value")

    result.show
    val result_collect = result.rdd.collect()



    // 将结果写入到tidb, 需要调整为upsert
    var dbConn = DbUtils.getDBConnection
    dbConn.setAutoCommit(false)
    val sql =
      s"""
         |insert into iot_ana_prov_cust_data_info
         |(gather_date, cust_id, gather_type, gather_value)
         |values (?,?,?,?)
         |on duplicate key update gather_value=?
       """.stripMargin

    val pstmt = dbConn.prepareStatement(sql)

    var i = 0
    for(r<-result_collect){
      val gather_date = dataTime
      val cust_id = r(0).toString
      val gather_type = r(1).toString
      val gather_value = r(2).toString.toDouble

      pstmt.setString(1, gather_date)
      pstmt.setString(2, cust_id)
      pstmt.setString(3, gather_type)
      pstmt.setDouble(4, gather_value)
      pstmt.setDouble(5, gather_value)

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

    // 更新断点时间
    CommonUtils.updateBreakTable("iot_ana_prov_cust_data_info", dataTime)


  }

}
