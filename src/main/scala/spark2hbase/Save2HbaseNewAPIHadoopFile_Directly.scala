package spark2hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import spark2hbase.Save2HbaseNewAPIHadoopFile.{columnsName, parse}

/**
  * Created by hadoop on 18-12-20.
  */
object Save2HbaseNewAPIHadoopFile_Directly {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val fileSystem = FileSystem.get(new Configuration)
    val input = sc.getConf.get("spark.app.input", "/user/slview/Dpi/S1uhttp")
    val hFilePath = sc.getConf.get("spark.app.hFilePath", "/tmp/tmptest2")
    val appName = sc.getConf.get("spark.app.name", "Save2HbaseNewAPIHadoopFile_20181220")
    val tableName = sc.getConf.get("spark.app.table", "spark2hbase_test")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)

    val conf = HBaseConfiguration.create()

    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val conn = ConnectionFactory.createConnection(conf)
    //根据表名获取表
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    //获取hbase表的region分布
    val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))


    val job = Job.getInstance(conf)
    job.setJobName("DumpFile")
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
        cn = columnsName(i).getBytes() //列的名称
        v = Bytes.toBytes(x(i+1).toString) //列的值
        kv = new KeyValue(rowkey, cf, cn, v) //封装一下 rowkey, cf, clounmVale, value
        kvlist = kvlist :+ kv //将新的kv加在kvlist后面（不能反 需要整体有序）
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

    println("==========================a================")
    result.sortBy(x=>{
      (x._1,x._2.toString.split("/")(0),x._2.toString.split("/")(1))
    }, true ).saveAsNewAPIHadoopFile("/tmp/tmptest2", classOf[ImmutableBytesWritable], classOf[KeyValue],
      classOf[HFileOutputFormat2], job.getConfiguration())

  }
}
