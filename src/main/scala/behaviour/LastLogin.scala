package behaviour

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

/**
  * @author Tracy
  * @date 2021/6/23 10:05
  */
object LastLogin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("lastLogin")
      .master("local")
      .getOrCreate()

    //    读数据
    def catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_users"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"lastLoginTime":{"cf":"cf","col":"lastLoginTime","type":"string"}
        |}}
      """.stripMargin

    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

//    readDF.show(20)

    //  写数据
    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"lastLoginTime":{"cf":"cf","col":"lastLoginTime","type":"string"}
        |}}
      """.stripMargin

    readDF.write
      .option(HBaseTableCatalog.tableCatalog,catalogwrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

//    查看结果，需注释上面写操作再查看
    spark.read
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .show()


  }
}
