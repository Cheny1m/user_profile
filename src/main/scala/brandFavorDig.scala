import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.execution.columnar.STRUCT
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.{DataTypes, LongType}
import org.apache.log4j.{Level, Logger}

object brandFavorDig {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("brandRecommendation")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val model = ALSModel.load("model/brandFavorModel")

    val predicttoStringFunc = udf(predicttoString _)

    // 为推荐1个品牌
    //    val result: DataFrame = model.recommendForAllUsers(1)
    //      .withColumn("favorBrand", predict2StringFunc('recommendations))
    //      .withColumnRenamed("userId", "id")
    //      .drop('recommendations)
    //      .select('id.cast(LongType), 'favorBrand)
    //
    //    result.groupBy('favorBrand).count().show()

    //推荐多个品牌
    val result: DataFrame = model.recommendForAllUsers(2)
      .withColumn("favorBrands", predicttoStringFunc('recommendations))
      .withColumnRenamed("userId", "id")
      .drop('recommendations)
      .select('id.cast(LongType), 'favorBrands)


    val separator = ","
    lazy val first = result.first()

    val numAttrs = first.toString().split(separator).length
    val attrs = Array.tabulate(numAttrs)(n => "favorBrand_" + n)
    //按指定分隔符拆分value列，生成splitCols列
    var recommendationDF = result.withColumn("splitCols", split($"favorBrands", separator))
    attrs.zipWithIndex.foreach(x => {
      recommendationDF = recommendationDF.withColumn(x._1, $"splitCols".getItem(x._2))
    })
    val tempDF1 = recommendationDF
      .drop("favorBrands")
      .drop("splitCols")
      .drop("favorBrand_2")

    tempDF1.toDF().createOrReplaceTempView("tempTBL")

    val tempDF2 = tempDF1.select(
      'id,
      when('favorBrand_0 === 297, "海尔")
        .when('favorBrand_0 === 298, "卡萨帝")
        .when('favorBrand_0 === 299, "统帅")
        .when('favorBrand_0 === 305, "MOOKA")
        .otherwise("其他").as('favorBrand_0),
//      when('favorBrand_1 === 297, "海尔")
//        .when('favorBrand_1 === 298, "卡萨帝")
//        .when('favorBrand_1 === 299, "统帅")
//        .when('favorBrand_1 === 305, "MOOKA")
//        .otherwise("其他").as('favorBrand_1),
        when('favorBrand_2 === 297, "海尔")
        .when('favorBrand_2 === 298, "卡萨帝")
        .when('favorBrand_2 === 299, "统帅")
        .when('favorBrand_2 === 305, "MOOKA")
        .otherwise("其他").as('favorBrand_1)
    )

    tempDF2.show(100, false)

    def recommendationCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"user_profile"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"Long"},
         |     "favorBrand_0":{"cf":"cf", "col":"favorBrand_0", "type":"string"},
         |     "favorBrand_1":{"cf":"cf", "col":"favorBrand_1", "type":"string"}
         |   }
         |}""".stripMargin

    tempDF2.write
      .option(HBaseTableCatalog.tableCatalog, recommendationCatalog)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

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
    arr.map(_.getAs[Int]("brandId")).mkString(",")
  }
}