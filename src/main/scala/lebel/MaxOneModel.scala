package net.suncaper.tags.model

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

//单笔最高标签
object MaxOneModel {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "orderAmount":{"cf":"cf", "col":"orderAmount", "type":"string"}
         |  }
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    /*val DF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/tags_dat")
      .option("dbtable", "tbl_orders")
      .option("user", "root")
      .option("password", "123456")
      .option("partitionColumn", "id")
      .option("lowerBound", 1)
      .option("upperBound", 60)
      .option("numPartitions", 10)
      .load()
      .groupBy('memberId)
      .agg(max('orderAmount) as "max_Amount")

    val  result = DF.select('memberId,
      when('max_Amount>=1 and('max_Amount<=999),"1-999")
        .when('max_Amount>=1000 and('max_Amount<=2999),"1000-2999")
        .when('max_Amount>=3000 and('max_Amount<=4999),"3000-4999")
        .when('max_Amount>=5000 and('max_Amount<=9999),"5000-9999")
        .otherwise("10000以上")
        .as("max_Amount")
    )
      .na.drop(List("max_Amount"))
      .show()*/

    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val source2 = source.select('id, 'memberId, 'orderAmount.cast(DataTypes.IntegerType))
    val temp =  source2
      //.withColumn("memberId",regexp_extract('memberId,"\\d+",0))
      .groupBy('memberId)
      .agg(max('orderAmount) as "max_Amount")
      .orderBy('max_Amount.desc)
      val  result = temp.select('memberId,
        when('max_Amount>=1 and('max_Amount<=999),"1-999")
          .when('max_Amount>=1000 and('max_Amount<=2999),"1000-2999")
          .when('max_Amount>=3000 and('max_Amount<=4999),"3000-4999")
          .when('max_Amount>=5000 and('max_Amount<=9999),"5000-9999")
          .otherwise("10000以上")
          .as("max_Amount")
      )
      .na.drop(List("max_Amount"))

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

    val res = source_user.join(result,source_user.col("id")===result.col("memberId"),"left")
      .select('id,'max_Amount)
    res
      .where('memberId===19)
      .show(100, false)
    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |  "max_Amount":{"cf":"cf", "col":"max_Amount", "type":"string"}
         |}
         |}""".stripMargin

    res.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
