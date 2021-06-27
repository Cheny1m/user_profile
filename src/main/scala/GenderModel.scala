
//533 995 128
//
//533 995 128


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object GenderModel {
  def main(args: Array[String]): Unit = {
    def catalog = s"""{
                     |"table":{"namespace":"default", "name":"tbl_users"},
                     |"rowkey":"id",
                     |"columns":{
                     |"id":{"cf":"rowkey", "col":"id", "type":"string"},
                     |"user_agent":{"cf":"cf", "col":"job", "type":"string"}
                     |}
                     |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .show(10,false)
  }
}