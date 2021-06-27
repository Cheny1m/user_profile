package net.suncaper.tags.model

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
//消费周期标签，已测试
object ConsumptionCycleModel {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "payTime":{"cf":"cf", "col":"payTime", "type":"string"}
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
      .agg(((max('payTime) - min('payTime))/(count('payTime)-1)) as "internal")
      .na.drop(List("internal"))
      .orderBy('internal)
    val result = DF.select('memberId,
      when('internal < 60 * 60* 24 * 7 ,"1周")
        .when('internal < 60 * 60* 24 * 7 *2
          and('internal > 60 * 60* 24 * 7),"2周")
        .when('internal < 60 * 60* 24 * 30
          and('internal > 60 * 60* 24 * 7 * 2),"1月")
        .when('internal < 60 * 60* 24 * 30 * 2
          and('internal > 60 * 60* 24 * 30),"2月")
        .when('internal < 60 * 60* 24 * 30 * 3
          and('internal > 60 * 60* 24 * 30 * 2),"3月")
        .when('internal < 60 * 60* 24 * 30 * 4
          and('internal > 60 * 60* 24 * 30 * 3),"4月")
        .when('internal < 60 * 60* 24 * 30 * 5
          and('internal > 60 * 60* 24 * 30 * 4),"5月")
        .when('internal < 60 * 60* 24 * 30 * 6
          and('internal > 60 * 60* 24 * 30 * 5),"6月")
        .otherwise("半年以上")
        .as('internal)
    )
      .show()*/
    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()


    val temp = source
      .groupBy('memberId)
      .agg(((max('payTime) - min('payTime))/(count('payTime)-1)) as "internal")
      .na.drop(List("internal"))
    val result = temp.select('memberId,
      when('internal < 60 * 60* 24 * 7 ,"1周")
        .when('internal < 60 * 60* 24 * 7 *2
          and('internal > 60 * 60* 24 * 7),"2周")
        .when('internal < 60 * 60* 24 * 30
          and('internal > 60 * 60* 24 * 7 * 2),"1月")
        .when('internal < 60 * 60* 24 * 30 * 2
          and('internal > 60 * 60* 24 * 30),"2月")
        .when('internal < 60 * 60* 24 * 30 * 3
          and('internal > 60 * 60* 24 * 30 * 2),"3月")
        .when('internal < 60 * 60* 24 * 30 * 4
          and('internal > 60 * 60* 24 * 30 * 3),"4月")
        .when('internal < 60 * 60* 24 * 30 * 5
          and('internal > 60 * 60* 24 * 30 * 4),"5月")
        .when('internal < 60 * 60* 24 * 30 * 6
          and('internal > 60 * 60* 24 * 30 * 5),"6月")
        .otherwise("半年以上")
        .as('internal)
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

    val res = source_user.join(result,source_user.col("id")===result.col("memberId"),"left")
      .select('id,'internal)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |  "internal":{"cf":"cf", "col":"internal", "type":"string"}
         |}
         |}""".stripMargin
    res.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
