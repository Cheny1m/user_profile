package net.suncaper.tags.model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType}

object tblDataShowTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("tblDataTest")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    //    def userCatalog =
    //      s"""{
    //         |  "table":{"namespace":"default", "name":"tbl_users"},
    //         |  "rowkey":"id",
    //         |   "columns":{
    //         |     "id":{"cf":"rowkey", "col":"id", "type":"Long"}
    //         |   }
    //         |}""".stripMargin
    //    val userDF: DataFrame = spark.read
    //      .option(HBaseTableCatalog.tableCatalog, userCatalog)
    //      .format("org.apache.spark.sql.execution.datasources.hbase")
    //      .load()
    //    userDF.toDF().createOrReplaceTempView("users")

    def productCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_goods"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"Long"},
         |     "orderId":{"cf":"cf", "col":"orderId", "type":"string"},
         |     "productType":{"cf":"cf", "col":"productType", "type":"string"},
         |     "productId":{"cf":"cf", "col":"productId", "type":"string"},
         |     "productName":{"cf":"cf", "col":"productName", "type":"string"}
         |   }
         |}""".stripMargin

    val productDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, productCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    productDF.toDF().createOrReplaceTempView("products")

    //    def orderCatalog =
    //      s"""{
    //         |  "table":{"namespace":"default", "name":"tbl_orders"},
    //         |  "rowkey":"id",
    //         |   "columns":{
    //         |     "id":{"cf":"rowkey", "col":"id", "type":"Long"},
    //         |     "memberId":{"cf":"cf", "col":"memberId", "type":"string"}
    //         |   }
    //         |}""".stripMargin
    //    val orderDF: DataFrame = spark.read
    //      .option(HBaseTableCatalog.tableCatalog, orderCatalog)
    //      .format("org.apache.spark.sql.execution.datasources.hbase")
    //      .load()
    //    orderDF.toDF().createOrReplaceTempView("orders")

    def logsCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_logs"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"Long"},
         |     "global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |     "loc_url":{"cf":"cf", "col":"loc_url", "type":"string"}
         |   }
         |}""".stripMargin

    val logsDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, logsCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    logsDF.toDF().createOrReplaceTempView("logs")

    val url2ProductId = udf(getProductId _)

    val productIdDF = logsDF.select(
      'global_user_id.as("userId").cast(DataTypes.IntegerType),
      url2ProductId('loc_url).as("productId").cast(DataTypes.IntegerType)
    ).filter('productId.isNotNull)

    productIdDF.toDF().createOrReplaceTempView("tblProductId")

    //    productDF.show()
    //
    //    productIdDF.show()

    val resultOfProductAndUser = spark
      .sql("SELECT userId as id, tblProductId.productId as pid,productType as ptype, productName as pname FROM tblProductId LEFT OUTER JOIN products ON tblProductId.productId = products.productId")
      .filter('pname.isNotNull)
      .groupBy('id, 'ptype)
      .agg(count('ptype) as "rating")

    //    userId as id, tblProductId.productId as pid,productType as ptype, productName as pname

    //    resultOfProductAndUser.show(100, false)

    val als = new ALS()
      .setUserCol("id")
      .setItemCol("ptype")
      .setRatingCol("rating")
      .setPredictionCol("predict")
      .setColdStartStrategy("drop")
      .setAlpha(5)
      .setMaxIter(5)
      .setRank(5)
      .setRegParam(0.1)
      .setImplicitPrefs(true)

    val model: ALSModel = als.fit(resultOfProductAndUser)

    model.save("model/product/als")

//    val model = ALSModel.load("model/product/als")
//
//    val predict2StringFunc = udf(predict2String _)
//
//    val result: DataFrame = model.recommendForAllUsers(10)
//      .withColumn("favorType", predict2StringFunc('recommendations))
//      .withColumnRenamed("userId", "id")
//      .drop('recommendations)
//      .select('id.cast(LongType), 'favorType)
//
//    result.show(100, false)
//
//    def recommendationTypeCatalog =
//      s"""{
//         |  "table":{"namespace":"default", "name":"typeRecomTest"},
//         |  "rowkey":"id",
//         |   "columns":{
//         |     "id":{"cf":"rowkey", "col":"id", "type":"Long"},
//         |     "favorType":{"cf":"cf", "col":"favorType", "type":"string"}
//         |   }
//         |}""".stripMargin
//
//    result.write
//      .option(HBaseTableCatalog.tableCatalog, recommendationTypeCatalog)
//      .option(HBaseTableCatalog.newTable, "5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()
//
    spark.stop()
  }

  def getProductId(url: String) = {
    val productId = new StringBuilder()
    if (url.contains("/product/") && url.contains(".html")) {
      val start: Int = url.indexOf("/product/")
      val end: Int = url.indexOf(".html")
      if (end > start) {
        productId.append(url.substring(start + 9, end))
      }
    }
    productId
  }

  def predict2String(arr: Seq[Row]) = {
    arr.map(_.getAs[Int]("productId")).mkString(",")
  }
}
