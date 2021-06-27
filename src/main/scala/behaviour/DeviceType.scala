package behaviour

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

/**
  * @author Tracy
  * @date 2021/6/23 15:16
  */

//行为特征：设备类型

object DeviceType {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("deviceType")
      .master("local")
      .getOrCreate()

    import spark.implicits._

//    读数据
    def catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_logs"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"global_user_id":{"cf":"cf","col":"global_user_id","type":"string"},
        |"user_agent":{"cf":"cf","col":"user_agent","type":"string"}
        |}}
      """.stripMargin

    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
//    readDF.show(20)

    var tempDF = readDF.select('global_user_id.cast("int") as "uid",'user_agent)
        .select('uid as "id",'user_agent)
//    tempDF.show()


    val result = tempDF.select('id,
        when('user_agent like "%Window%","Window")
          .when('user_agent like "%Android%","Android")
          .when('user_agent like "%Linux%","Linux")
          .when('user_agent like "%iPhone%","IOS")
          .when('user_agent like "%iPad%","IOS")
          .when('user_agent like "%Mac%","Mac")
          .otherwise("")
          .as("deviceType"))
      .where('deviceType =!= "")
      .sort('id)
      .distinct()

//    result.show(950)


//    写入mysql
    result
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_deviceType")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

//    查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_deviceType")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show(950)



    spark.stop()
  }
}
