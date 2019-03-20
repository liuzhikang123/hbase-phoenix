package spark2hbase

import hbasePhoenix.IotDpiUserS1U_cut._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
/**
  * Created by hadoop on 18-12-20.
  */
object Save2HbaseNewAPIHadoopFile_df {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val input = sc.getConf.get("spark.app.input", "/user/slview/Dpi/S1uhttp")
    val hFilePath = sc.getConf.get("spark.app.hFilePath", "/tmp/tmptest1")
    val appName = sc.getConf.get("spark.app.name", "Save2HbaseNewAPIHadoopFile_20181220")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    val conf = HBaseConfiguration.create()
    val tableName = "spark2hbase_test"
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



    /**
      * 将DataFrame 保存为 HFile
      *
      * @param resultDataFrame 需要保存为HFile的 DataFrame，DataFrame的第一个字段必须为"key"
      * @param clounmFamily 列族名称（必须在Hbase中存在，否则在load数据的时候会失败）
      * @param hFilePath HFile的保存路径
      */
    def saveASHfFile(resultDataFrame: DataFrame, clounmFamily: String, hFilePath: String): Unit = {
      println("===================b=========")
      var columnsName: Array[String] = resultDataFrame.columns //获取列名 第一个为key
      columnsName = columnsName.drop(1).sorted //把key去掉  因为要排序
      import spark.implicits._
      println("===================bb=========")
      val result1: RDD[(ImmutableBytesWritable, Seq[KeyValue])] = resultDataFrame.map(row => {
        println("===================bbbbb=========")
        var kvlist: Seq[KeyValue] = List()
        println("===================ccccc=========")
        var rowkey: Array[Byte] = null
        var cn: Array[Byte] = null
        var v: Array[Byte] = null
        var kv: KeyValue = null
        val cf: Array[Byte] = clounmFamily.getBytes //列族
        rowkey = Bytes.toBytes(row.getAs[String]("key")) //key
        for (i <- 1 until columnsName.length) {
          cn = columnsName(i).getBytes() //列的名称
          v = Bytes.toBytes(row.getAs[String](columnsName(i))) //列的值
          //将rdd转换成HFile需要的格式,我们上面定义了Hfile的key是ImmutableBytesWritable,
          //那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
          kv = new KeyValue(rowkey, cf, cn, v)
          println("===================ddddd=========")
          kvlist = kvlist :+ kv //将新的kv加在kvlist后面（不能反 需要整体有序）
          println("===================eeeee=========")
        }
        (new ImmutableBytesWritable(rowkey), kvlist)
      }).rdd

      //RDD[(ImmutableBytesWritable, Seq[KeyValue])] 转换成 RDD[(ImmutableBytesWritable, KeyValue)]
      val result: RDD[(ImmutableBytesWritable, KeyValue)] = result1.flatMapValues(s => {
        s.iterator
      })

      //删除hFile路径
      if (fileSystem.exists(new Path(hFilePath))) {
        fileSystem.delete(new Path(hFilePath), true)
      }

      //保存数据
      result.sortBy(x=>{
          (x._1,x._2.toString.split("/")(0),x._2.toString.split("/")(1))
        }, true ) //要保持 整体有序
        .saveAsNewAPIHadoopFile(hFilePath,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        job.getConfiguration)

    }



    val rowRdd = sc.textFile(input + "/"+dataTime+"/").map(x => parse(x)).filter(_.length!=1)
    val df = sqlContext.createDataFrame(rowRdd, struct)
    val resultDataFrame = df.selectExpr("concat(reverse(MSISDN) + '_' + StartTime + '_' + EndTime) as key","*")
    println("===================aaaaa=========")
    saveASHfFile(resultDataFrame, "cf1", hFilePath)


    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path(hFilePath), table.asInstanceOf[HTable])
  }

}
