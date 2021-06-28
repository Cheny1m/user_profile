package MLmeasure

//import RecommendationModel.predict2String
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.execution.columnar.STRUCT
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType}

//商品偏好挖掘模型，基于ALS算法，通过tbl_logs表中用户访问商品的次数来打分
object productFavorModel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GenderName")
      .master("local")
      .getOrCreate()

    import spark.implicits._

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

//    println("hello")
    ratingDF.show(10)
    val als = new ALS()
      .setUserCol("userId")
      .setItemCol("productId")
      .setRatingCol("rating")
      .setPredictionCol("predict")
      .setColdStartStrategy("drop")
      .setAlpha(10)
      .setMaxIter(10)
      .setRank(10)
      .setRegParam(1.0)
      .setImplicitPrefs(true)

    // 将数据集切分为两份，其中训练集占80%(0.8), 测试集占20%(0.2)
    val Array(trainSet, testSet) = ratingDF.randomSplit(Array(0.8, 0.2))

    // 回归模型评测器
    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setLabelCol("rating")
      .setPredictionCol("predict")
      .setMetricName("rmse")

//    trainSet.show(10, false)
    // 通过训练集进行训练，建立模型
    val model: ALSModel = als.fit(trainSet)

    model.save("model/productFavorModel")

    // 通过模型进行预测
    val predictions = model.transform(testSet)

    val rmse = evaluator.evaluate(predictions)

    println(s"rmse value is ${rmse}")


    spark.stop()
  }

  def getProductId(url: String) = {
    var productId = new String()
    if (url.contains("/product/") && url.contains(".html")) {
      val start: Int = url.indexOf("/product/")
      val end: Int = url.indexOf(".html")
//      println("start:",start,"end:",end)
      if (end > start) {
        productId += url.substring(start + 9, end)
      }
    }
    productId
  }

  def predicttoString(arr: Seq[Row]) = {
    arr.map(_.getAs[Int]("productId")).mkString(",")
  }
}
