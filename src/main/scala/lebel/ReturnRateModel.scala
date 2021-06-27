package net.suncaper.tags.model

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
//退货率标签，暂定orderStatus:0是退货,已完成
object ReturnRateModel {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "orderStatus":{"cf":"cf", "col":"orderStatus", "type":"string"}
         |  }
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

   /* val DF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/tags_dat")
      .option("dbtable", "tbl_orders")
      .option("user", "root")
      .option("password", "123456")
      .option("partitionColumn", "id")
      .option("lowerBound", 1)
      .option("upperBound", 60)
      .option("numPartitions", 10)
      .load()
      .na.drop(List("orderStatus"))

      val temp = DF
      .groupBy('memberId)
      .agg(count('orderStatus) as "count")

    val temp2 = DF
      .where('orderStatus===0)
      .groupBy('memberId)
      .agg(count('orderStatus) as "ReturnCount")

    val result = temp.join(temp2,temp.col("memberId")===temp2.col("memberId"))
      .withColumn("returnRate",col("ReturnCount")/col("count"))
      .select(temp.col("memberId"),
        when('returnRate>0.05,"高")
          .when('returnRate<=0.05
            and('returnRate>0.009),"中")
          .otherwise("低")
          .as("returnRate")
      ).show()*/



    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .drop("id")


    val temp = source
      .groupBy('memberId as "idd")
      .agg(count('orderStatus) as "count")

    val temp2 = source
      .where('orderStatus===200)
      .groupBy('memberId)
      .agg(count('orderStatus) as "ReturnCount")

    val result = temp.join(temp2,temp.col("idd")===temp2.col("memberId"))
      .withColumn("returnRate",col("ReturnCount")/col("count"))
      .select(temp.col("idd"),
        when('returnRate>0.05,"高")
          .when('returnRate<=0.05
            and('returnRate>0.009),"中")
          .otherwise("低")
          .as("returnRate")
      )


    def catalog_user =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_users"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"}
         |  }
         |}""".stripMargin

    val source_user: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_user)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val res = source_user.join(result,source_user.col("id")===result.col("idd"),"left")
      .select('id,'returnRate)
      .orderBy('id)
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |  "returnRate":{"cf":"cf", "col":"returnRate", "type":"string"}
         |}
         |}""".stripMargin

    res.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
