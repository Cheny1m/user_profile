package net.suncaper.tags.model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object EmailModel {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("Email")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_users"},
         |  "rowkey":"id",
         |    "columns":{
         |      "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |      "email":{"cf":"cf", "col":"email", "type":"string"}
         |    }
         |}""".stripMargin

    val source = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val result = source.select('id, 'email)

    result.show()

    def writecatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"user_profile"},
         |  "rowkey":"id",
         |    "columns":{
         |      "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |      "email":{"cf":"cf", "col":"email", "type":"string"}
         |    }
         |}""".stripMargin

    result.write
      .option(HBaseTableCatalog.tableCatalog, writecatalog)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    spark.stop()
  }
}
