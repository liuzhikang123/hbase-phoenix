package spark2hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
  * Created on 下午10:43.
  */
object Save2Hbase {
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
    val data = rdd.map(x => parsehttp(x))

    data.saveAsHadoopDataset(jobConf)

    sc.stop()
  }
  def parsehttp(line:String) = {
    val fields = line.split("\\|", -1).filter(_.length!=1)

    val Interface = if(fields(0).length > 0) fields(0) else "-1"
    val IMSI = if(fields(1).length > 0) fields(1) else "-1"
    val MSISDN = if(fields(2).length > 0) fields(2) else "-1"
    val IMEI = if(fields(3).length > 0) fields(3) else "-1"
    val APN = if(fields(4).length > 0) fields(4) else "-1"
    val DestinationIP = if(fields(5).length > 0) fields(5) else "-1"
    val DestinationPort = if(fields(6).length > 0) fields(6) else "-1"
    val SourceIP = if(fields(7).length > 0) fields(7) else "-1"
    val SourcePort = if(fields(8).length > 0) fields(8) else "-1"
    val SGWIP = if(fields(9).length > 0) fields(9) else "-1"
    val MMEIP = if(fields(10).length > 0) fields(10) else "-1"
    val PGWIP = if(fields(11).length > 0) fields(11) else "-1"
    val ECGI = if(fields(12).length > 0) fields(12) else "-1"
    val TAI = if(fields(13).length > 0) fields(13) else "-1"
    val VisitedPLMNId = if(fields(14).length > 0) fields(14) else "-1"
    val RATType = if(fields(15).length > 0) fields(15) else "-1"
    val ProtocolID = if(fields(16).length > 0) fields(16) else "-1"
    val ServiceType = if(fields(17).length > 0) fields(17) else "-1"
    val DestinationType = if(fields(18).length > 0) fields(18) else "-1"
    val chargingID = if(fields(19).length > 0) fields(19) else "-1"
    val StartTime = if(fields(20).length > 0) fields(20) else "-1"
    val EndTime = if(fields(21).length > 0) fields(21) else "-1"

    val mdnReverse = MSISDN.reverse
    val rowkey = mdnReverse + "_" + StartTime + "_" + EndTime

    val put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Interface"), Bytes.toBytes(Interface))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("IMSI"), Bytes.toBytes(IMSI))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("MSISDN"), Bytes.toBytes(MSISDN))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("IMEI"), Bytes.toBytes(IMEI))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("APN"), Bytes.toBytes(APN))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("DestinationIP"), Bytes.toBytes(DestinationIP))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("DestinationPort"), Bytes.toBytes(DestinationPort))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("SourceIP"), Bytes.toBytes(SourceIP))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("SourcePort"), Bytes.toBytes(SourcePort))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("SGWIP"), Bytes.toBytes(SGWIP))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("MMEIP"), Bytes.toBytes(MMEIP))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("PGWIP"), Bytes.toBytes(PGWIP))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ECGI"), Bytes.toBytes(ECGI))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("TAI"), Bytes.toBytes(TAI))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("VisitedPLMNId"), Bytes.toBytes(VisitedPLMNId))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("RATType"), Bytes.toBytes(RATType))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ProtocolID"), Bytes.toBytes(ProtocolID))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ServiceType"), Bytes.toBytes(ServiceType))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("DestinationType"), Bytes.toBytes(DestinationType))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("chargingID"), Bytes.toBytes(chargingID))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("StartTime"), Bytes.toBytes(StartTime))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("EndTime"), Bytes.toBytes(EndTime))

    (new ImmutableBytesWritable, put)
  }
}
