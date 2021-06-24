package consumption

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

object maxAmount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("maxAmount")
      .master("local")
      .getOrCreate()

    import  spark.implicits._



    val readDF = spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat")
      .option("dbtable","user_maxAmount")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
//    readDF.show()

    val result = readDF.select('id,'maxAmount as "amount")
      .select('id,
        when('amount >= 50000 ,"50000及以上")
        .when('amount >= 20000,"20000-49999")
        .when('amount >= 10000,"10000-19999")
        .when('amount >= 5000, "5000-9999")
        .when('amount >= 1, "1-4999")
        .otherwise("未消费")
        .as("maxAmount"))

//    result.show()

    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"use_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"maxAmount":{"cf":"cf","col":"maxAmount","type":"string"}
        |}}
      """.stripMargin
    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .option(HBaseTableCatalog.newTable,"5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

//    spark.read
//      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//      .show(950,false)



    spark.stop()
  }

}
