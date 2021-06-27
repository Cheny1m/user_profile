package net.suncaper.tags.model


import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
//国籍标签,已测试
object NationalityModel {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"long"},
         |"nationality":{"cf":"cf", "col":"nationality", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    /*val DF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/tags_dat")
      .option("dbtable", "tbl_users")
      .option("user", "root")
      .option("password", "123456")
      .option("partitionColumn", "id")
      .option("lowerBound", 1)
      .option("upperBound", 60)
      .option("numPartitions", 10)
      .load()
      .select('id,'username,
        when('nationality === "1", "中国大陆")
          .when('nationality === "2", "中国香港")
          .when('nationality === "3", "中国台湾")
          .when('nationality === "4", "中国澳门")
          .otherwise("其他")
          .as("nationality")
      ).na.drop(List("nationality"))
      .show()*/
    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val result = readDF.select('id,
      when('nationality === "1", "中国大陆")
        .when('nationality === "2", "中国香港")
        .when('nationality === "3", "中国台湾")
        .when('nationality === "4", "中国澳门")
        .otherwise("其他")
        .as("nationality")
    ).na.drop(List("nationality"))

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"nationality":{"cf":"cf", "col":"nationality", "type":"string"}
         |}
         |}""".stripMargin

    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
