package hbasePhoenix

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by liuzk on 18-8-22.
  */
object IotDpiSignS6A {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val input = sc.getConf.get("spark.app.input", "/user/slview/Dpi/S6a") //
    val htable = sc.getConf.get("spark.app.htable", "IOT_DPI_SIGN_S6A_20180829")
    val output = sc.getConf.get("spark.app.output", "/user/slview/Dpi/S6a_phoenix")
    val repartitionNum = sc.getConf.get("spark.app.repartitionNum", "1").toInt
    val appName = sc.getConf.get("spark.app.name")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    val rowRdd = sc.textFile(input + "/"+dataTime+"/").map(x => parse(x)).filter(_.length!=1)
    val df = sqlContext.createDataFrame(rowRdd, struct)

    df.filter("length(MSISDN)>0").repartition(repartitionNum).write.format("orc").mode(SaveMode.Overwrite)
      .save(output+"/"+dataTime)

    val newDf = sqlContext.read.format("orc").load(output+"/"+dataTime)

    newDf.selectExpr(
      "Interface",
      "XDR_ID",
      "IMSI",
      "IMEI",
      "MSISDN",

      "RAT",
      "Home_Province",
      "Roma_Province",
      "Procedure_Type",
      "Procedure_Start_Time",
      "Procedure_End_Time",

      "Procedure_Status",
      "Result_Code",
      "Machine_IP_Add_type",
      "MME_Address",
      "HSS_Address",

      "MME_Port",
      "HSS_Port",
      "Subscriber_Status",
      "Access_Restriction_Data",
      "Default_APN",

      "UE_AMBR_UL",
      "UE_AMBR_DL",
      "USER_IPv4",
      "USER_IPv6",
      "Origin_Host",

      "Destination_Host",
      "Application_ID"
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
    StructField("XDR_ID", StringType),
    StructField("IMSI", StringType),
    StructField("IMEI", StringType),
    StructField("MSISDN", StringType),

    StructField("RAT", StringType),
    StructField("Home_Province", StringType),
    StructField("Roma_Province", StringType),
    StructField("Procedure_Type", StringType),
    StructField("Procedure_Start_Time", StringType),
    StructField("Procedure_End_Time", StringType),

    StructField("Procedure_Status", StringType),
    StructField("Result_Code", StringType),
    StructField("Machine_IP_Add_type", StringType),
    StructField("MME_Address", StringType),
    StructField("HSS_Address", StringType),

    StructField("MME_Port", StringType),
    StructField("HSS_Port", StringType),
    StructField("Subscriber_Status", StringType),
    StructField("Access_Restriction_Data", StringType),
    StructField("Default_APN", StringType),

    StructField("UE_AMBR_UL", StringType),
    StructField("UE_AMBR_DL", StringType),
    StructField("USER_IPv4", StringType),
    StructField("USER_IPv6", StringType),
    StructField("Origin_Host", StringType),

    StructField("Destination_Host", StringType),
    StructField("Application_ID", StringType)

  ))

  def parse(line: String) = {
    try {
      val fields = line.split("\\|",-1)

      val Interface = fields(0)
      val XDR_ID = fields(1)
      val IMSI = fields(2)
      val IMEI = fields(3)
      val MSISDN = fields(4)

      val RAT = fields(5)
      val Home_Province = fields(6)
      val Roma_Province = fields(7)
      val Procedure_Type = fields(8)
      val Procedure_Start_Time = fields(9)
      val Procedure_End_Time = fields(10)

      val Procedure_Status = fields(11)
      val Result_Code = fields(12)
      val Machine_IP_Add_type = fields(13)
      val MME_Address = fields(14)
      val HSS_Address = fields(15)

      val MME_Port = fields(16)
      val HSS_Port = fields(17)
      val Subscriber_Status = fields(18)
      val Access_Restriction_Data = fields(19)
      val Default_APN = fields(20)

      val UE_AMBR_UL = fields(21)
      val UE_AMBR_DL = fields(22)
      val USER_IPv4 = fields(23)
      val USER_IPv6 = fields(24)
      val Origin_Host = fields(25)

      val Destination_Host = fields(26)
      val Application_ID = fields(27)


      val mdnReverse = MSISDN.reverse

      Row(Interface,XDR_ID,IMSI,IMEI,MSISDN,
        RAT,Home_Province,Roma_Province,Procedure_Type,Procedure_Start_Time,Procedure_End_Time,
        Procedure_Status,Result_Code,Machine_IP_Add_type,MME_Address,HSS_Address,
        MME_Port,HSS_Port,Subscriber_Status,Access_Restriction_Data,Default_APN,
        UE_AMBR_UL,UE_AMBR_DL,USER_IPv4,USER_IPv6,Origin_Host,
        Destination_Host,Application_ID)

    } catch {
      case e: Exception => {
        Row("0")
      }
    }
  }

}
