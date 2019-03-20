package hbasePhoenix

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  *
  * Created by liuzk on 18-8-22.
  */
object IotDpiUserS1UMQTT {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    //val spark = SparkSession.builder().appName("Spark2SavePhoenixDF_201809190623").master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val input = sc.getConf.get("spark.app.input", "/user/slview/Dpi/S1umqtt") //
    val htable = sc.getConf.get("spark.app.htable", "IOT_DPI_USER_S1U_MQTT_20180829")
    val output = sc.getConf.get("spark.app.output", "/user/slview/Dpi/S1umqtt_phoenix")
    val repartitionNum = sc.getConf.get("spark.app.repartitionNum", "1").toInt
    val appName = sc.getConf.get("spark.app.name")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    val rowRdd = sc.textFile(input + "/"+dataTime+"/").map(x => parse(x)).filter(_.length!=1)
    val df = sqlContext.createDataFrame(rowRdd, struct)
    //.filter("MSISDN like '86106498966' and APN rlike 'ctnb'")
    df.filter("length(MSISDN)>0 and APN like '%ctnb%'").repartition(repartitionNum).write.format("orc").mode(SaveMode.Overwrite)
      .save(output+"/"+dataTime)

    val newDf = sqlContext.read.format("orc").load(output+"/"+dataTime)

    newDf.selectExpr(
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
      "Roma_Province",// new add
      //"UP_TTL",// new add
      //"DOWN_TTL",// new add
      "Version",
      "ProcedureType",
      "ClientID",
      "UserName",
      "MQTT",
      "KeepAlive",
      "DupFlag",
      "Qos",
      "Protocol_Name",
      "Topic_Name",
      "Topic2_Name",
      "Topic3_Name",
      "Message_Action",
      "MsgActionAck",
      "MessageID",
      "RepeatCount",
      "MsgActionTime",
      "MsgActionAck_Time",
      "Pubreceived_Time",
      "Pubrelease_Time",
      "PubComplete_Time",
      "Status",
      "Result"
    ).write.format("org.apache.phoenix.spark").
      mode(SaveMode.Overwrite).options( Map("table" -> htable,
      "zkUrl" -> zkUrl)).save()

    val dpiPath = new Path(input + "/"+dataTime)
    val dpiPath_done = new Path(input + "/"+dataTime+"_done")

    if (!fileSystem.exists(dpiPath_done.getParent)) {
      fileSystem.mkdirs(dpiPath_done.getParent)
    }
    fileSystem.rename(dpiPath, dpiPath_done)

  }
  val struct = StructType(Array(
    StructField("Interface", StringType),
    StructField("IMSI", StringType),
    StructField("MSISDN", StringType),
    StructField("IMEI", StringType),
    StructField("APN", StringType),
    StructField("DestinationIP", StringType),
    StructField("DestinationPort", StringType),
    StructField("SourceIP", StringType),
    StructField("SourcePort", StringType),
    StructField("SGWIP", StringType),
    StructField("MMEIP", StringType),
    StructField("PGWIP", StringType),
    StructField("ECGI", StringType),
    StructField("TAI", StringType),
    StructField("VisitedPLMNId", StringType),
    StructField("RATType", StringType),
    StructField("ProtocolID", StringType),
    StructField("ServiceType", StringType),
    StructField("XXX1", StringType),
    StructField("XXX2", StringType),
    StructField("StartTime", StringType),
    StructField("EndTime", StringType),
    StructField("Duration", StringType),
    StructField("InputOctets", StringType),
    StructField("OutputOctets", StringType),
    StructField("InputPacket", StringType),
    StructField("OutputPacket", StringType),
    StructField("PDNConnectionId", StringType),
    StructField("BearerID", StringType),
    StructField("BearerQoS", StringType),
    StructField("RecordCloseCause", StringType),
    StructField("ENBIP", StringType),
    StructField("SGWPort", StringType),
    StructField("eNBPort", StringType),
    StructField("eNBGTP_TEID", StringType),
    StructField("SGWGTP_TEID", StringType),
    StructField("PGWPort", StringType),
    StructField("MME_UE_S1AP_ID", StringType),
    StructField("eNB_UE_S1AP_ID", StringType),
    StructField("MME_Group_ID", StringType),
    StructField("MME_Code", StringType),
    StructField("eNB_ID", StringType),
    StructField("Home_Province", StringType),
    StructField("UserIP", StringType),
    StructField("UserPort", StringType),
    StructField("L4protocal", StringType),
    StructField("AppServerIP", StringType),
    StructField("AppServer_Port", StringType),
    StructField("ULTCPReorderingPacket", StringType),
    StructField("DLTCPReorderingPacket", StringType),
    StructField("ULTCP_RetransPacket", StringType),
    StructField("DLTCP_RetransPacket", StringType),
    StructField("TCPSetupResponseDelay", StringType),
    StructField("TCPSetupACKDelay", StringType),
    StructField("ULIPFragPacks", StringType),
    StructField("DLIPFragPacks", StringType),
    StructField("Delay_Setup_FirstTransaction", StringType),
    StructField("Delay_FirstTransaction_FirstResPackt", StringType),
    StructField("WindowSize", StringType),
    StructField("MSSSize", StringType),
    StructField("TCPSynNumber", StringType),
    StructField("TCPConnetState", StringType),
    StructField("SessionStopFlag", StringType),
    StructField("Roma_Province", StringType),// new add
    StructField("UP_TTL", StringType),// new add
    StructField("DOWN_TTL", StringType),// new add
    StructField("Version", StringType),
    StructField("ProcedureType", StringType),
    StructField("ClientID", StringType),
    StructField("UserName", StringType),
    StructField("MQTT", StringType),
    StructField("KeepAlive", StringType),
    StructField("DupFlag", StringType),
    StructField("Qos", StringType),
    StructField("Protocol_Name", StringType),
    StructField("Topic_Name", StringType),
    StructField("Topic2_Name", StringType),
    StructField("Topic3_Name", StringType),
    StructField("Message_Action", StringType),
    StructField("MsgActionAck", StringType),
    StructField("MessageID", StringType),
    StructField("RepeatCount", StringType),
    StructField("MsgActionTime", StringType),
    StructField("MsgActionAck_Time", StringType),
    StructField("Pubreceived_Time", StringType),
    StructField("Pubrelease_Time", StringType),
    StructField("PubComplete_Time", StringType),
    StructField("Status", StringType),
    StructField("Result", StringType)

  ))

  def parse(line: String) = {
    try {
      val fields = line.split("\\|",-1)

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
      val XXX1 = fields(18)
      val XXX2 = fields(19)
      val StartTime = fields(20)
      val EndTime = fields(21)
      val Duration = fields(22)
      val InputOctets = fields(23)
      val OutputOctets = fields(24)
      val InputPacket = fields(25)
      val OutputPacket = fields(26)
      val PDNConnectionId = fields(27)
      val BearerID = fields(28)
      val BearerQoS = fields(29)
      val RecordCloseCause = fields(30)
      val ENBIP = fields(31)
      val SGWPort = fields(32)
      val eNBPort = fields(33)
      val eNBGTP_TEID = fields(34)
      val SGWGTP_TEID = fields(35)
      val PGWPort = fields(36)
      val MME_UE_S1AP_ID = fields(37)
      val eNB_UE_S1AP_ID = fields(38)
      val MME_Group_ID = fields(39)
      val MME_Code = fields(40)
      val eNB_ID = fields(41)
      val Home_Province = fields(42)
      val UserIP = fields(43)
      val UserPort = fields(44)
      val L4protocal = fields(45)
      val AppServerIP = fields(46)
      val AppServer_Port = fields(47)
      val ULTCPReorderingPacket = fields(48)
      val DLTCPReorderingPacket = fields(49)
      val ULTCP_RetransPacket = fields(50)
      val DLTCP_RetransPacket = fields(51)
      val TCPSetupResponseDelay = fields(52)
      val TCPSetupACKDelay = fields(53)
      val ULIPFragPacks = fields(54)
      val DLIPFragPacks = fields(55)
      val Delay_Setup_FirstTransaction = fields(56)
      val Delay_FirstTransaction_FirstResPackt = fields(57)
      val WindowSize = fields(58)
      val MSSSize = fields(59)
      val TCPSynNumber = fields(60)
      val TCPConnetState = fields(61)
      val SessionStopFlag = fields(62)
      val Roma_Province = fields(63)// new add
      val UP_TTL = fields(64)// new add
      val DOWN_TTL = fields(65)// new add
      val Version = fields(66)
      val ProcedureType = fields(67)
      val ClientID = fields(68)
      val UserName = fields(69)
      val MQTT = fields(70)
      val KeepAlive = fields(71)
      val DupFlag = fields(72)
      val Qos = fields(73)
      val Protocol_Name = fields(74)
      val Topic_Name = fields(75)
      val Topic2_Name = fields(76)
      val Topic3_Name = fields(77)
      val Message_Action = fields(78)
      val MsgActionAck = fields(79)
      val MessageID = fields(80)
      val RepeatCount = fields(81)
      val MsgActionTime = fields(82)
      val MsgActionAck_Time = fields(83)
      val Pubreceived_Time = fields(84)
      val Pubrelease_Time = fields(85)
      val PubComplete_Time = fields(86)
      val Status = fields(87)
      val Result = fields(88)


      val mdnReverse = MSISDN.reverse

      Row(Interface,IMSI,MSISDN,IMEI,APN,
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
        TCPSynNumber,TCPConnetState,SessionStopFlag,Roma_Province,UP_TTL,DOWN_TTL,//new add
        Version,ProcedureType,ClientID,UserName,MQTT,
        KeepAlive,DupFlag,Qos,Protocol_Name,Topic_Name,
        Topic2_Name,Topic3_Name,Message_Action,MsgActionAck,MessageID,
        RepeatCount,MsgActionTime,MsgActionAck_Time,Pubreceived_Time,Pubrelease_Time,
        PubComplete_Time,Status,Result)

    } catch {
      case e: Exception => {
        Row("0")
      }
    }
  }

}
