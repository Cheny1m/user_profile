package MLmeasure

import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.execution.columnar.STRUCT
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType}
import org.apache.log4j.{Level, Logger}

object brandFavorModel {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("brandFavor")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    def productCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_goods"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"Long"},
         |     "orderId":{"cf":"cf", "col":"orderId", "type":"string"},
         |     "productType":{"cf":"cf", "col":"productType", "type":"string"},
         |     "productId":{"cf":"cf", "col":"productId", "type":"string"},
         |     "brandId":{"cf":"cf", "col":"brandId", "type":"string"},
         |     "productName":{"cf":"cf", "col":"productName", "type":"string"}
         |   }
         |}""".stripMargin

    val productDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, productCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    productDF.toDF().createOrReplaceTempView("products")

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

    val urltoProductId = udf(getProductId _)

    val logsDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, logsCatalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val ratingDF = logsDF.select(
      'global_user_id.as("userId").cast(DataTypes.IntegerType),
      urltoProductId('loc_url).as("productId").cast(DataTypes.IntegerType)
    ).filter('productId.isNotNull)
      .groupBy('userId, 'productId)
      .agg(count('productId) as "rating")

    ratingDF.toDF().createOrReplaceTempView("ratingDF")

    val tempDF = spark
      .sql("select ratingDF.userId, brandId, rating from products LEFT OUTER JOIN ratingDF ON products.productId = ratingDF.productId")
      .na.drop(List("rating"))

    val tempDF2 = tempDF.select('userId,
      'brandId .cast(DataTypes.IntegerType),
      'rating
    )

    //    tempDF2.show(100, false)

    val als = new ALS()
      .setUserCol("userId")
      .setItemCol("brandId")
      .setRatingCol("rating")
      .setPredictionCol("predict")
      .setColdStartStrategy("drop")
      .setAlpha(10)
      .setMaxIter(10)
      .setRank(10)
      .setRegParam(1.0)
      .setImplicitPrefs(true)

    val model: ALSModel = als.fit(tempDF2)

    model.save("model/brandFavorModel")
    println("savesavesave!!!!!!!!!!!!!!!!!!!!!!")

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

  def predicttoString(arr: Seq[Row]) = {
    arr.map(_.getAs[Int]("productId")).mkString(",")
  }

}