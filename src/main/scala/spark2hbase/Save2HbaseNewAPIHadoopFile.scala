package spark2hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by liuzk on 18-12-20.
  *
  * scan 'spark2hbase_test',{LIMIT=>3}
    truncate 'spark2hbase_test'
    count 'spark2hbase_test'
  */
object Save2HbaseNewAPIHadoopFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val fileSystem = FileSystem.get(new Configuration)
    val input = sc.getConf.get("spark.app.input", "/user/slview/Dpi/S1uhttp")
    val appName = sc.getConf.get("spark.app.name", "Save2HbaseNewAPIHadoopFile_20181220")
    val hFilePath = sc.getConf.get("spark.app.hFilePath", "/tmp/tmptest1")
    val tableName = sc.getConf.get("spark.app.table", "spark2hbase_test")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    val conf = HBaseConfiguration.create()
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("mapred.output.compress", "true")
    hadoopConf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    hadoopConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
    conf.set("mapred.output.compress", "true")
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    conf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)

    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val conn = ConnectionFactory.createConnection(conf)
    //根据表名获取表
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    //获取hbase表的region分布
    val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))


    val job = Job.getInstance(conf)
    job.setJobName("makeHFile")
    //需要设置文件输出的key,因为我们要生成HFile,所以outkey要用ImmutableBytesWritable
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    //输出文件的内容KeyValue
    job.setMapOutputValueClass(classOf[KeyValue])
    //配置HFileOutputFormat2的信息
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)


    val rdd = sc.textFile(input + "/"+dataTime+"/").map(x => parse(x)).filter(_.length!=1)

    val result1 = rdd.map(x => {
      var kvlist: Seq[KeyValue] = List()
      var cn: Array[Byte] = null
      var v: Array[Byte] = null
      var kv: KeyValue = null
      val cf: Array[Byte] = "cf1".getBytes //列族
      val rowkey = Bytes.toBytes(x(0).toString)
      for (i <- columnsName.indices) {
        cn = columnsName(i).getBytes() //列名
        v = Bytes.toBytes(x(i+1).toString) //列值
        kv = new KeyValue(rowkey, cf, cn, v) //封装一下 rowkey, cf, cn, v
        kvlist = kvlist :+ kv //将新的kv加在kvlist后面
      }
      (new ImmutableBytesWritable(rowkey), kvlist)
    })

    val result: RDD[(ImmutableBytesWritable, KeyValue)] = result1.flatMapValues(s => {
      s.iterator
    })



    //删除hFile路径
    if (fileSystem.exists(new Path(hFilePath))) {
      fileSystem.delete(new Path(hFilePath), true)
    }

    println("==================sorting================")
    result.sortBy(x=>{
        (x._1,x._2.toString.split("/")(0),x._2.toString.split("/")(1))
      }, true )
      .saveAsNewAPIHadoopFile(hFilePath, classOf[ImmutableBytesWritable], classOf[KeyValue],
        classOf[HFileOutputFormat2], conf)

    //Bulk load Hfiles to Hbase
    //hbase的数据文件是保存在region的hfile上，通过直接写入数据到hfile，
    //并将hfile保存到hbase中，这种方式可以直接饶过region的split和compact，速度更快。

    //------在制作HFile文件的时候，一定要(主键+列族+列)排序。Put进去会自动排序。但自己做成HFile文件不会自动排序。
    //------没解决，使用KeyValue 放到一个List里面，然后FlatMap一下

    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path(hFilePath), table.asInstanceOf[HTable])
  }
  val columnsName = Array(
    "Interface",
    "IMSI",
    "MSISDN",
    "IMEI",
    "APN",

    "DestinationIP",
    "DestinationPort",
    "SourceIP",
    "SourcePort",
    "SGWIP",

    "MMEIP",
    "PGWIP",
    "ECGI",
    "TAI",
    "VisitedPLMNId",

    "RATType",
    "ProtocolID",
    "ServiceType",
    "XXX1",
    "XXX2",

    "StartTime",
    "EndTime",
    "Duration",
    "InputOctets",
    "OutputOctets",

    "InputPacket",
    "OutputPacket",
    "PDNConnectionId",
    "BearerID",
    "BearerQoS",

    "RecordCloseCause",
    "ENBIP",
    "SGWPort",
    "eNBPort",
    "eNBGTP_TEID",

    "SGWGTP_TEID",
    "PGWPort",
    "MME_UE_S1AP_ID",
    "eNB_UE_S1AP_ID",
    "MME_Group_ID",

    "MME_Code",
    "eNB_ID",
    "Home_Province",
    "UserIP",
    "UserPort",

    "L4protocal",
    "AppServerIP",
    "AppServer_Port",
    "ULTCPReorderingPacket",
    "DLTCPReorderingPacket",

    "ULTCP_RetransPacket",
    "DLTCP_RetransPacket",
    "TCPSetupResponseDelay",
    "TCPSetupACKDelay",
    "ULIPFragPacks",

    "DLIPFragPacks",
    "Delay_Setup_FirstTransaction",
    "Delay_FirstTransaction_FirstResPackt",
    "WindowSize",
    "MSSSize",

    "TCPSynNumber",
    "TCPConnetState",
    "SessionStopFlag",
    "Roma_Province")



  def parse(line: String) = {
    try {
      val fields = line.split("\\|",-1)

      val Interface = if(fields(0).length>0) fields(0) else "-1"
      val IMSI = if(fields(1).length>0) fields(1) else "-1"
      val MSISDN = if(fields(2).length>0) fields(2) else "-1"
      val IMEI = if(fields(3).length>0) fields(3) else "-1"
      val APN = if(fields(4).length>0) fields(4) else "-1"

      val DestinationIP = if(fields(5).length>0) fields(5) else "-1"
      val DestinationPort = if(fields(6).length>0) fields(6) else "-1"
      val SourceIP = if(fields(7).length>0) fields(7) else "-1"
      val SourcePort = if(fields(8).length>0) fields(8) else "-1"
      val SGWIP = if(fields(9).length>0) fields(9) else "-1"

      val MMEIP = if(fields(10).length>0) fields(10) else "-1"
      val PGWIP = if(fields(11).length>0) fields(11) else "-1"
      val ECGI = if(fields(12).length>0) fields(12) else "-1"
      val TAI = if(fields(13).length>0) fields(13) else "-1"
      val VisitedPLMNId = if(fields(14).length>0) fields(14) else "-1"

      val RATType = if(fields(15).length>0) fields(15) else "-1"
      val ProtocolID = if(fields(16).length>0) fields(16) else "-1"
      val ServiceType = if(fields(17).length>0) fields(17) else "-1"
      val XXX1 = if(fields(18).length>0) fields(18) else "-1"
      val XXX2 = if(fields(19).length>0) fields(19) else "-1"

      val StartTime = if(fields(20).length>0) fields(20) else "-1"
      val EndTime = if(fields(21).length>0) fields(21) else "-1"
      val Duration = if(fields(22).length>0) fields(22) else "-1"
      val InputOctets = if(fields(23).length>0) fields(23) else "-1"
      val OutputOctets = if(fields(24).length>0) fields(24) else "-1"

      val InputPacket = if(fields(25).length>0) fields(25) else "-1"
      val OutputPacket = if(fields(26).length>0) fields(26) else "-1"
      val PDNConnectionId = if(fields(27).length>0) fields(27) else "-1"
      val BearerID = if(fields(28).length>0) fields(28) else "-1"
      val BearerQoS = if(fields(29).length>0) fields(29) else "-1"

      val RecordCloseCause = if(fields(30).length>0) fields(30) else "-1"
      val ENBIP = if(fields(31).length>0) fields(31) else "-1"
      val SGWPort = if(fields(32).length>0) fields(32) else "-1"
      val eNBPort = if(fields(33).length>0) fields(33) else "-1"
      val eNBGTP_TEID = if(fields(34).length>0) fields(34) else "-1"

      val SGWGTP_TEID = if(fields(35).length>0) fields(35) else "-1"
      val PGWPort = if(fields(36).length>0) fields(36) else "-1"
      val MME_UE_S1AP_ID = if(fields(37).length>0) fields(37) else "-1"
      val eNB_UE_S1AP_ID = if(fields(38).length>0) fields(38) else "-1"
      val MME_Group_ID = if(fields(39).length>0) fields(39) else "-1"

      val MME_Code = if(fields(40).length>0) fields(40) else "-1"
      val eNB_ID = if(fields(41).length>0) fields(41) else "-1"
      val Home_Province = if(fields(42).length>0) fields(42) else "-1"
      val UserIP = if(fields(43).length>0) fields(43) else "-1"
      val UserPort = if(fields(44).length>0) fields(44) else "-1"

      val L4protocal = if(fields(45).length>0) fields(45) else "-1"
      val AppServerIP = if(fields(46).length>0) fields(46) else "-1"
      val AppServer_Port = if(fields(47).length>0) fields(47) else "-1"
      val ULTCPReorderingPacket = if(fields(48).length>0) fields(48) else "-1"
      val DLTCPReorderingPacket = if(fields(49).length>0) fields(49) else "-1"

      val ULTCP_RetransPacket = if(fields(50).length>0) fields(50) else "-1"
      val DLTCP_RetransPacket = if(fields(51).length>0) fields(51) else "-1"
      val TCPSetupResponseDelay = if(fields(52).length>0) fields(52) else "-1"
      val TCPSetupACKDelay = if(fields(53).length>0) fields(53) else "-1"
      val ULIPFragPacks = if(fields(54).length>0) fields(54) else "-1"

      val DLIPFragPacks = if(fields(55).length>0) fields(55) else "-1"
      val Delay_Setup_FirstTransaction = if(fields(56).length>0) fields(56) else "-1"
      val Delay_FirstTransaction_FirstResPackt = if(fields(57).length>0) fields(57) else "-1"
      val WindowSize = if(fields(58).length>0) fields(58) else "-1"
      val MSSSize = if(fields(59).length>0) fields(59) else "-1"

      val TCPSynNumber = if(fields(60).length>0) fields(60) else "-1"
      val TCPConnetState = if(fields(61).length>0) fields(61) else "-1"
      val SessionStopFlag = if(fields(62).length>0) fields(62) else "-1"
      val Roma_Province = if(fields(63).length>0) fields(63) else "-1"


      val mdnReverse = MSISDN.reverse

      Row(mdnReverse + StartTime + EndTime,Interface,IMSI,MSISDN,IMEI,APN,
        DestinationIP,DestinationPort,SourceIP,SourcePort,SGWIP,
        MMEIP,PGWIP,ECGI,TAI,VisitedPLMNId,
        RATType,ProtocolID,ServiceType,XXX1,XXX2,
        StartTime,EndTime,Duration,InputOctets,OutputOctets,
        InputPacket,OutputPacket,PDNConnectionId,BearerID,BearerQoS,
        RecordCloseCause,ENBIP,SGWPort,eNBPort,eNBGTP_TEID,
        SGWGTP_TEID,PGWPort,MME_UE_S1AP_ID,eNB_UE_S1AP_ID,MME_Group_ID,
        MME_Code,eNB_ID,Home_Province,UserIP,UserPort,
        L4protocal,AppServerIP,AppServer_Port,ULTCPReorderingPacket,DLTCPReorderingPacket,
        ULTCP_RetransPacket,DLTCP_RetransPacket,TCPSetupResponseDelay,TCPSetupACKDelay,ULIPFragPacks,
        DLIPFragPacks,Delay_Setup_FirstTransaction,Delay_FirstTransaction_FirstResPackt,WindowSize,MSSSize,
        TCPSynNumber,TCPConnetState,SessionStopFlag,Roma_Province)

    } catch {
      case e: Exception => {
        Row("0")
      }
    }
  }

}
