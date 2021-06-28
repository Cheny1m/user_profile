package behaviour

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}

object urlBrowse {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("urlBrowse")
      .master("local")
      .getOrCreate()

    import spark.implicits._


    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"loc_url":{"cf":"cf", "col":"loc_url", "type":"string"}
         |}
         |}""".stripMargin

    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .select('global_user_id.cast("int") as "id",'loc_url)

    val result = readDF
      .groupBy('id, 'loc_url)
      .agg(count('loc_url) as "count")
      .withColumn("row_num", row_number() over Window.partitionBy('id).orderBy('count.asc))
      .where('row_num === 1)
      .drop("count", "row_num")

//    result.show(950,false)


    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"urlBrowse":{"cf":"cf","col":"urlBrowse","type":"string"}
        |}}
      """.stripMargin
    result
      .where('id <= 950)
      .select('id.cast("string") as "id",'loc_url as "urlBrowse")
      .write
      .option(HBaseTableCatalog.tableCatalog,catalogwrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()




    spark.stop()
  }

}
