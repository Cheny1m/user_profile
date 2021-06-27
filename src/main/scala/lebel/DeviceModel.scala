package net.suncaper.tags.model

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

//设备标签
object DeviceModel {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"user_agent":{"cf":"cf", "col":"user_agent", "type":"string"}
         |}
         |}""".stripMargin


    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._



       /* val DF = spark.read.format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/tags_dat")
          .option("dbtable", "tbl_logs")
          .option("user", "root")
          .option("password", "123456")
          .option("partitionColumn", "id")
          .option("lowerBound", 1)
          .option("upperBound", 60)
          .option("numPartitions", 10)
          .load()

        val result = DF.select('global_user_id,
          when('user_agent like "%Android%","Android")
          .when('user_agent like "%Linux%","Linux")
            .when('user_agent like "%Windows%","Windows")
            .when('user_agent like "%Macintosh%","Mac")
            .when('user_agent like "%iPhone%" or('user_agent like "%iPad%"),"IOS")
            .as("device")
        ).na.drop(List("device")).show()*/



    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

      val result = readDF.select('global_user_id,
          when('user_agent like "%Android%","Android")
          .when('user_agent like "%Linux%","Linux")
            .when('user_agent like "%Windows%","Windows")
            .when('user_agent like "%Macintosh%","Mac")
            .when('user_agent like "%iPhone%" or('user_agent like "%iPad%"),"IOS")
            .as("device")
        ).na.drop(List("device"))

    def catalog_user =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_users"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"}
         |  }
         |}""".stripMargin

    val source_user: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_user)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val res = source_user.join(result,source_user.col("id")===result.col("global_user_id"),"left")
      .select('id,'device)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"device":{"cf":"cf", "col":"device", "type":"string"}
         |}
         |}""".stripMargin

    res.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
