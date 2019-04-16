package hbasePhoenix

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by liuzk on 18-8-22.
  */
object IotDpiSignS5S8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    // val spark = SparkSession.builder().appName("Spark2SavePhoenixDF").master("local[*]").getOrCreate()
    //  val spark = SparkSession.builder().enableHiveSupport().appName("name_123-201805221210").master("local[3]").getOrCreate()
    //val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val zkUrl = sc.getConf.get("spark.app.zkurl", "10.37.28.39,10.37.28.41,10.37.28.42:2181")
    val input = sc.getConf.get("spark.app.input", "/user/slview/Dpi/S5S8") //
    val htable = sc.getConf.get("spark.app.htable", "IOT_DPI_SIGN_S5S8_20180829")
    val output = sc.getConf.get("spark.app.output", "/user/slview/Dpi/S5S8_phoenix")
    val repartitionNum = sc.getConf.get("spark.app.repartitionNum", "1").toInt
    val appName = sc.getConf.get("spark.app.name")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    val dirSize = fileSystem.getContentSummary(new Path(input + "/"+dataTime+"/")).getLength
    //println("------------------------" + dirSize + "------------------------")

    if(dirSize < 1*1024*1024*1024){
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
        "Roma_Province",//新增
        "Procedure_Type",
        "Procedure_Start_Time",
        "Procedure_End_Time",

        "Procedure_Status",
        "Result_Code",
        "APN",
        "APN_AMBR_UL",
        "APN_AMBR_DL",

        "USER_IPv4",
        "USER_IPv6",
        "Machine_IP_Add_type",
        "SGW_IP_Add",
        "PGW_IP_Add",

        "SGW_Port",
        "PGW_Port",
        "SGW_Control_TEID",
        "PGW_Control_TEID",
        "TAI",

        "ECGI",
        "ULI_Length",
        "ULI",
        "EPS_Bearer_Number",
        "Bearer_Context"
      ).write.format("org.apache.phoenix.spark").
        mode(SaveMode.Overwrite).options( Map("table" -> htable,
        "zkUrl" -> zkUrl)).save()

      val dpiPath = new Path(input + "/"+dataTime)
      val dpiPath_done = new Path(input + "/"+dataTime+"_done")

      if (!fileSystem.exists(dpiPath_done.getParent)) {
        fileSystem.mkdirs(dpiPath_done.getParent)
      }
      fileSystem.rename(dpiPath, dpiPath_done)
    }else{
      println("------current minute filesize is bigger than 1G---------")
    }



  }
  val struct = StructType(Array(
    StructField("Interface", StringType),
    StructField("XDR_ID", StringType),
    StructField("IMSI", StringType),
    StructField("IMEI", StringType),
    StructField("MSISDN", StringType),

    StructField("RAT", StringType),
    StructField("Home_Province", StringType),
    StructField("Roma_Province", StringType),//新增
    StructField("Procedure_Type", StringType),
    StructField("Procedure_Start_Time", StringType),
    StructField("Procedure_End_Time", StringType),

    StructField("Procedure_Status", StringType),
    StructField("Result_Code", StringType),
    StructField("APN", StringType),
    StructField("APN_AMBR_UL", StringType),
    StructField("APN_AMBR_DL", StringType),

    StructField("USER_IPv4", StringType),
    StructField("USER_IPv6", StringType),
    StructField("Machine_IP_Add_type", StringType),
    StructField("SGW_IP_Add", StringType),
    StructField("PGW_IP_Add", StringType),

    StructField("SGW_Port", StringType),
    StructField("PGW_Port", StringType),
    StructField("SGW_Control_TEID", StringType),
    StructField("PGW_Control_TEID", StringType),
    StructField("TAI", StringType),

    StructField("ECGI", StringType),
    StructField("ULI_Length", StringType),
    StructField("ULI", StringType),
    StructField("EPS_Bearer_Number", IntegerType),
    StructField("Bearer_Context", StringType)

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
      val APN = fields(13)
      val APN_AMBR_UL = fields(14)
      val APN_AMBR_DL = fields(15)

      val USER_IPv4 = fields(16)
      val USER_IPv6 = fields(17)
      val Machine_IP_Add_type = fields(18)
      val SGW_IP_Add = fields(19)
      val PGW_IP_Add = fields(20)

      val SGW_Port = fields(21)
      val PGW_Port = fields(22)
      val SGW_Control_TEID = fields(23)
      val PGW_Control_TEID = fields(24)
      val TAI = fields(25)

      val ECGI = fields(26)
      val ULI_Length = fields(27)
      val ULI = fields(28)


      val len = fields.length

      var EPS_Bearer_Number = 0
      var start = 29

      try{
        EPS_Bearer_Number = if(len>start) fields(start).toInt else 0
      }catch {
        case e:Exception => {
          EPS_Bearer_Number = 0
        }
      }

      var Bearer_Context = ""

      var index = 0

      for(i <- start until len){
        if(index % 8 == 0){
          Bearer_Context = Bearer_Context + "-" + fields(i)
        } else{
          Bearer_Context = Bearer_Context + "," + fields(i)
        }

        index = index + 1
        if(index == len){
          Bearer_Context = Bearer_Context.substring(1)
        }
      }



      val mdnReverse = MSISDN.reverse

      Row(Interface,XDR_ID,IMSI,IMEI,MSISDN,
        RAT,Home_Province,Roma_Province,Procedure_Type,Procedure_Start_Time,Procedure_End_Time,
        Procedure_Status,Result_Code,APN,APN_AMBR_UL,APN_AMBR_DL,
        USER_IPv4,USER_IPv6,Machine_IP_Add_type,SGW_IP_Add,PGW_IP_Add,
        SGW_Port,PGW_Port,SGW_Control_TEID,PGW_Control_TEID,TAI,
        ECGI,ULI_Length,ULI,EPS_Bearer_Number,Bearer_Context)

    } catch {
      case e: Exception => {
        Row("0")
      }
    }
  }

}
