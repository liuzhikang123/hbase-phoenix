package com.zyuc.iot.HA_PGW_OFFLINE

import java.sql.PreparedStatement

import com.zyuc.dpi.java.utils.DbUtils
import com.zyuc.dpi.utils.CommonUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by hadoop on 19-1-4.
  */
object Offilne_3g {
  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder().master("local[2]").appName("test").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sparkConf = spark.sparkContext.getConf
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val crmpath = sparkConf.get("spark.app.crmpath", "/user/slview/CRM/data/d=20180510/")
    //val bs4gpath = sparkConf.get("spark.app.bs4gpath", "/user/slview/hadoop/basic/bsid/4g/")
    val bs4gpath = sparkConf.get("spark.app.bs4gpath", "/user/slview/IotBSInfo/data")
    //val bs3gpath = sparkConf.get("spark.app.bs3gpath", "/user/slview/hadoop/basic/bsid/3g")
    val bs3gpath = sparkConf.get("spark.app.bs3gpath", "/user/slview/pdsnSID/pdsnSID.txt")
    val haradiuspath =  sparkConf.get("spark.app.haradius", "/user/slview/hadoop/haradius/data")
    val pgwradiuspath =  sparkConf.get("spark.app.pgwradiuspath", "/user/slview/hadoop/pgwradius/data")
    val stattime = sparkConf.get("spark.app.stattime", "201812281415") //201812281415 201812281515
    val sevenDaysAgo = sparkConf.get("spark.app.sevenDaysAgo", "201812211415")
    val haradiusout =  sparkConf.get("spark.app.haradiusout", "/user/slview/hadoop/haradius/out")
    val pgwradiusout =  sparkConf.get("spark.app.pgwradiusout", "/user/slview/hadoop/pgwradius/out")
    val nbradiusout =  sparkConf.get("spark.app.nbradiusout", "/user/slview/hadoop/nbradius/out")
    val stattype = sparkConf.get("spark.app.stattype", "0") // 0-statall, 1-statha, 2-stat-pgw, 3-statnb
    val tidbha = sparkConf.get("spark.app.tidbha", "iot_ana_5min_3g_cust_offline")
    val tidbpgw = sparkConf.get("spark.app.tidbpgw", "iot_ana_5min_4g_cust_offline")
    val tidbnb = sparkConf.get("spark.app.tidbnb", "iot_ana_5min_nb_cust_offline")

    val inputpath3g = sparkConf.get("spark.app.inputpath3g", "/user/slview/HA_PGW_OFFLINE/3G")
    val inputpath4g = sparkConf.get("spark.app.inputpath4g", "/user/slview/HA_PGW_OFFLINE/4G")

    val d=stattime.substring(0, 8)
    val h = stattime.substring(8, 10)
    val m5 = stattime.substring(10, 12)

    val crmTable = "crm_" + stattime
    spark.read.format("orc").load(crmpath).selectExpr("mdn", "custid as cust_id").createOrReplaceTempView(crmTable)
    val crmCacheTable = "cache_crm_" + stattime
    spark.sql(
      s"""
         |cache table ${crmCacheTable}
         |as select * from ${crmTable}
       """.stripMargin)

    val bs4gTable ="bs4g"
    val bs3gTable = "bs3g"
    val pgwTable = "pgw_" + stattime
    val haTable = "ha_" + stattime

    def set3gBsTable() = {
      import spark.implicits._
      val bs3gDF = spark.sparkContext.textFile(bs3gpath).map(x=>x.split("\\t")).
        filter(_.length == 3).map(x=>(x(0), x(1), x(2))).toDF("sid", "provName", "cityName")
      bs3gDF.createOrReplaceTempView(bs3gTable)
    }

    def set4gBsTable() ={
      spark.read.format("orc").load(bs4gpath).createOrReplaceTempView(bs4gTable)
    }

    def statPgw() = {

      val pgwDF = spark.sql(
        s"""
           |select c.cust_id, nvl(b.provName,-1), nvl(b.cityName,-1), p.TerminateCause,
           |       count(*) as session_cnt,
           |       count(distinct p.mdn) as mdn_cnt
           |from  ${pgwTable} p, ${crmCacheTable} c, ${bs4gTable} b
           |where p.mdn = c.mdn and p.bsid = b.enbid and p.status='Stop' and p.apn not like '%ctnb%'
           |group by c.cust_id, b.provName, b.cityName, p.TerminateCause
       """.stripMargin)

      pgwDF.write.format("csv").mode(SaveMode.Overwrite).save(pgwradiusout)
      insert2Tidb(tidbpgw+"_"+d, pgwradiusout)
      CommonUtils.updateBreakTable(tidbpgw, stattime+"00")
    }

    def stat3g() = {

      val inputHaPath = inputpath3g + "/*/" + s"Offline_3G_${stattime}00.json"

      spark.read.format("json").load(inputHaPath).createOrReplaceTempView(haTable)

      val haDF = spark.sql(
        s"""
           |select c.cust_id, nvl(b.provName,-1), nvl(b.cityName,-1), p.TerminateCause,
           |       count(*) as session_cnt,
           |       count(distinct p.mdn) as mdn_cnt
           |from  ${haTable} p, ${crmCacheTable} c, ${bs3gTable} b
           |where p.mdn = c.mdn and p.sid = b.sid and p.status='Stop'
           |group by c.cust_id, b.provName, b.cityName, p.TerminateCause
       """.stripMargin)

      haDF.write.format("csv").mode(SaveMode.Overwrite).save(haradiusout)

      insert2Tidb(tidbha+"_"+d, haradiusout)
      //更新 断点表
      CommonUtils.updateBreakTable(tidbha, stattime+"00")
    }

    def statNb() = {
      val nbDF = spark.sql(
        s"""
           |select c.cust_id, nvl(b.provName,-1), nvl(b.cityName,-1), p.TerminateCause,
           |       count(*) as session_cnt,
           |       count(distinct p.mdn) as mdn_cnt
           |from  ${pgwTable} p, ${crmCacheTable} c, ${bs4gTable} b
           |where p.mdn = c.mdn and p.bsid = b.enbid and p.status='Stop' and p.apn like '%ctnb%'
           |group by c.cust_id, b.provName, b.cityName, p.TerminateCause
       """.stripMargin)

      nbDF.write.format("csv").mode(SaveMode.Overwrite).save(nbradiusout)
      insert2Tidb(tidbnb+"_"+d, nbradiusout)
      CommonUtils.updateBreakTable(tidbnb, stattime+"00")
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
      pstmt.setString(1, sevenDaysAgo)
      pstmt.executeUpdate()
      pstmt.close()

      // 执行insert操作
      dbConn.setAutoCommit(false)
      val sql =
        s"""
           |insert into ${table}
           |(datatime, cust_id, provName, cityName, TerminateCause, session_cnt, mdn_cnt)
           |values (?,?,?,?,?,?,?)
       """.stripMargin

      pstmt = dbConn.prepareStatement(sql)
      var i = 0
      for(r<-result){
        val cust_id = coverNull(r(0))
        val provName = coverNull(r(1))
        val cityName = coverNull(r(2))
        val TerminateCause = coverNull(r(3))
        val session_cnnt = r(4)
        val mdn_cnt = r(5)


        pstmt.setString(1, stattime+"00")
        pstmt.setString(2, cust_id)
        pstmt.setString(3, provName)
        pstmt.setString(4, cityName)
        pstmt.setString(5, TerminateCause)
        pstmt.setInt(6, session_cnnt.toString.toInt)
        pstmt.setInt(7, mdn_cnt.toString.toInt)


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

    if(stattype == "0"){
      set3gBsTable()
      set4gBsTable()
      //stat3g()
      statPgw()
      statNb()
      stat3g()
    }else if(stattype == "1"){
      set3gBsTable()
      stat3g()
    }else if(stattype == "2" || stattype == "3"){
      val inputPgwPath = pgwradiuspath + "/" + "d=" + d + "/h=" + h + "/m5=" + m5
      spark.read.format("json").load(inputpath4g + "/*/" + s"Offline_4G_${stattime}00.json" ).createOrReplaceTempView(pgwTable)
      set4gBsTable()
      if(stattype == "2"){
        statPgw()
      }else{
        statNb()
      }
    }


  }
}
