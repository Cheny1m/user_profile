package gr

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

/**
  * @author Tracy
  * @date 2021/6/23 15:52
  */
object LoginFrequency {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("lastLogin")
      .master("local")
      .getOrCreate()

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

    readDF.show(20)
  }
}
