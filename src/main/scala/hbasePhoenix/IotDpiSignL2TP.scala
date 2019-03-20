package hbasePhoenix

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by liuzk on 18-11-30.
  */
object IotDpiSignL2TP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val input = sc.getConf.get("spark.app.input", "/user/slview/Dpi/L2TP") //
    val htable = sc.getConf.get("spark.app.htable", "IOT_DPI_SIGN_L2TP_20181130")
    val output = sc.getConf.get("spark.app.output", "/user/slview/Dpi/L2TP_phoenix")
    val repartitionNum = sc.getConf.get("spark.app.repartitionNum", "1").toInt
    val appName = sc.getConf.get("spark.app.name")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    val rowRdd = sc.textFile(input + "/"+dataTime+"/").map(x => parse(x)).filter(_.length!=1)
    val df = sqlContext.createDataFrame(rowRdd, struct)

    df.filter("length(MSISDN)>0").repartition(repartitionNum).write.format("orc").mode(SaveMode.Overwrite)
      .save(output+"/"+dataTime)

    val newDf = sqlContext.read.format("orc").load(output+"/"+dataTime)

    newDf.selectExpr(
      //"Interface",
      //"XDR_ID",
      "IMSI",
      "IMEI",
      "MSISDN",
      "RAT",
      //"Home_Province",
      "Roma_Province",
      "Procedure_Type",
      "Procedure_Start_Time",
      "Procedure_End_Time",
      "Procedure_Status",

      "LAC_IP",
      "LNS_IP",
      "LAC_TUNNEL_ID",
      "LNS_TUNNEL_ID",
      "SESSION_START_TIME",

      "SESSION_END_TIME",
      "LAC_SESSION_ID",
      "LNS_SESSION_ID",
      "APN",
      "CallSerialNumber",

      "AUTH_TYPE",
      "AUTH_NAME",
      "AUTH_Status",
      "IPCP_User_IP",
      "IPCP_LNS_IP",

      "Result_Code",
      "Error_Code",
      "Error_Message"
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
    StructField("Interface", StringType),//不用
    StructField("XDR_ID", StringType),//不用
    StructField("IMSI", StringType),
    StructField("IMEI", StringType),
    StructField("MSISDN", StringType),
    StructField("RAT", StringType),
    StructField("Home_Province", StringType),//不用

    StructField("Roma_Province", StringType),
    StructField("Procedure_Type", StringType),
    StructField("Procedure_Start_Time", StringType),
    StructField("Procedure_End_Time", StringType),
    StructField("Procedure_Status", StringType),

    StructField("LAC_IP", StringType),
    StructField("LNS_IP", StringType),
    StructField("LAC_TUNNEL_ID", StringType),
    StructField("LNS_TUNNEL_ID", StringType),
    StructField("SESSION_START_TIME", StringType),

    StructField("SESSION_END_TIME", StringType),
    StructField("LAC_SESSION_ID", StringType),
    StructField("LNS_SESSION_ID", StringType),
    StructField("APN", StringType),
    StructField("CallSerialNumber", StringType),

    StructField("AUTH_TYPE", StringType),
    StructField("AUTH_NAME", StringType),
    StructField("AUTH_Status", StringType),
    StructField("IPCP_User_IP", StringType),
    StructField("IPCP_LNS_IP", StringType),

    StructField("Result_Code", StringType),
    StructField("Error_Code", StringType),
    StructField("Error_Message", StringType)
  ))

  def parse(line: String) = {
    try {
      val fields = line.split("\\|",-1)

      val Interface = fields(0)//不用
      val XDR_ID = fields(1)//不用
      val IMSI = fields(2)
      val IMEI = fields(3)
      val MSISDN = fields(4)
      val RAT = fields(5)
      val Home_Province = fields(6)//不用

      val Roma_Province = fields(7)
      val Procedure_Type = fields(8)
      val Procedure_Start_Time = fields(9)
      val Procedure_End_Time = fields(10)
      val Procedure_Status = fields(11)

      val LAC_IP = fields(12)
      val LNS_IP = fields(13)
      val LAC_TUNNEL_ID = fields(14)
      val LNS_TUNNEL_ID = fields(15)
      val SESSION_START_TIME = fields(16)

      val SESSION_END_TIME = fields(17)
      val LAC_SESSION_ID = fields(18)
      val LNS_SESSION_ID = fields(19)
      val APN = fields(20)
      val CallSerialNumber = fields(21)

      val AUTH_TYPE = fields(22)
      val AUTH_NAME = fields(23)
      val AUTH_Status = fields(24)
      val IPCP_User_IP = fields(25)
      val IPCP_LNS_IP = fields(26)

      val Result_Code = fields(27)
      val Error_Code = fields(28)
      val Error_Message = fields(29)


      val mdnReverse = MSISDN.reverse

      Row(Interface,XDR_ID,IMSI,IMEI,MSISDN,RAT,Home_Province,//  不用
        Roma_Province,Procedure_Type,Procedure_Start_Time,Procedure_End_Time,Procedure_Status,
        LAC_IP,LNS_IP,LAC_TUNNEL_ID,LNS_TUNNEL_ID,SESSION_START_TIME,
        SESSION_END_TIME,LAC_SESSION_ID,LNS_SESSION_ID,APN,CallSerialNumber,
        AUTH_TYPE,AUTH_NAME,AUTH_Status,IPCP_User_IP,IPCP_LNS_IP,
        Result_Code,Error_Code,Error_Message)

    } catch {
      case e: Exception => {
        Row("0")
      }
    }
  }

}
