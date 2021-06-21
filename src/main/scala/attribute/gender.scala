package attribute

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

object gender {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("gender")
      .master("local")
      .getOrCreate()

    import  spark.implicits._

//    读数据
    def catalog =
      """{
         |"table":{"namespace":"default","name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey","col":"id","type":"string"},
         |"gender":{"cf":"cf","col":"gender","type":"string"}
         |}}
       """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

//  数据处理
    val resDF= readDF.select('id,
      when('gender === "1","男").
        when('gender === "2","女").
        otherwise("未知").
        as("gender"))
//    println(readDF.count())
//    resDF.show()

//    写数据
    val catalogwrite =
      """
        |{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"gender":{"cf":"cf","col":"gender","type":"string"}
        |}}
      """.stripMargin
    resDF.write
        .option(HBaseTableCatalog.tableCatalog, catalogwrite)
        .option(HBaseTableCatalog.newTable,"5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()




    spark.stop()

  }

}
