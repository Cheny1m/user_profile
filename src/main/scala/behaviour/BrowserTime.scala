package behaviour

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

/**
  * @author Tracy
  * @date 2021/6/23 10:22
  */
object BrowserTime {
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
        |"log_time":{"cf":"cf","col":"log_time","type":"string"},
        |"log_id":{"cf":"cf","col":"log_id","type":"string"}
        |}}
      """.stripMargin

    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    readDF.show(20)
  }
}
