package spark2hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created on 下午10:43.
  */
object Save2Hbase_TablePut {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val input = sc.getConf.get("spark.app.input", "/user/slview/Dpi/S1uhttp") //
    val appName = sc.getConf.get("spark.app.name", "Save2Hbase_20181220")
    val tableName = sc.getConf.get("spark.app.table", "spark2hbase_test")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    //创建HBase配置
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "10.37.28.39,10.37.28.41,10.37.28.42")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")

    //创建JobConf，设置输出格式和表名
    val jobConf = new JobConf(hconf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    //rdd保存到HBase表
    val rdd = sc.textFile(input + "/"+dataTime+"/")
    val data = rdd.map(line=>line.split("\\|", -1))
      .map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),
      x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),
      x(20),x(21)))//.map(x => parsehttp(x))

    val columns = Array("Interface", "IMSI", "MSISDN", "IMEI", "APN",
      "DestinationIP", "DestinationPort", "SourceIP", "SourcePort", "SGWIP",
      "MMEIP", "PGWIP", "ECGI", "TAI", "VisitedPLMNId",
      "RATType", "ProtocolID", "ServiceType", "DestinationType", "chargingID",
      "StartTime", "EndTime")

    data.foreachPartition{
      x=> {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "10.37.28.39,10.37.28.41,10.37.28.42")
        myConf.set("hbase.zookeeper.property.clientPort", "2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, TableName.valueOf(tableName))
        myTable.setAutoFlush(false, false)//关键点1
        myTable.setWriteBufferSize(5*1024*1024)//关键点2

        x.foreach{ y => {
          val mdnReverse = y(0).toString.reverse
          val rowkey = mdnReverse + "_" + y(20) + "_" + y(21)
          val p = new Put(Bytes.toBytes(rowkey))

          for( i <- 0 to (columns.length - 1)){
            p.addColumn("cf1".getBytes, columns(i).getBytes, Bytes.toBytes(y(i).toString))
          }

          myTable.put(p)
        }
        }
        myTable.flushCommits()//关键点3
      }
    }

    sc.stop()
//    关键点1_:将自动提交关闭，如果不关闭，每写一条数据都会进行提交，是导入数据较慢的做主要因素。
//    关键点2:设置缓存大小，当缓存大于设置值时，hbase会自动提交。此处可自己尝试大小，一般对大数据量，设置为5M即可，本文设置为3M。
//    关键点3:每一个分片结束后都进行flushCommits()，如果不执行，当hbase最后缓存小于上面设定值时，不会进行提交，导致数据丢失。
  }
  def myparsecoap(line:String) = {
    val fields = line.split("\\|", -1).filter(_.length!=1)

    val Interface = fields(0)
    val IMSI = fields(1)
    val MSISDN = fields(2)
    val IMEI = fields(3)
    val APN = fields(4)
    val DestinationIP = fields(5)
    val DestinationPort = fields(6)
    val SourceIP = fields(7)
    val SourcePort = fields(8)
    val SGWIP = fields(9)
    val MMEIP = fields(10)
    val PGWIP = fields(11)
    val ECGI = fields(12)
    val TAI = fields(13)
    val VisitedPLMNId = fields(14)
    val RATType = fields(15)
    val ProtocolID = fields(16)
    val ServiceType = fields(17)
    val DestinationType = fields(18)
    val chargingID = fields(19)
    val StartTime = fields(20)
    val EndTime = fields(21)

    (Interface,IMSI,MSISDN,IMEI,APN,
      DestinationIP,DestinationPort,SourceIP,SourcePort,SGWIP,
      MMEIP,PGWIP,ECGI,TAI,VisitedPLMNId,
      RATType,ProtocolID,ServiceType,DestinationType,chargingID,
      StartTime,EndTime)
  }
}
