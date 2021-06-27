package attribute

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object username {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("username")
      .master("local")
      .getOrCreate()

    import  spark.implicits._


    def catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_users"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"username":{"cf":"cf","col":"username","type":"string"}
        |}}
      """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

//    readDF.show()


    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"username":{"cf":"cf","col":"username","type":"string"}
        |}}
      """.stripMargin
    readDF.write
      .option(HBaseTableCatalog.tableCatalog,catalogwrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()








    spark.stop()
  }

}
