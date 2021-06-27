
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType}

object categoryFavorDig {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("typeRecommendation")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    //    加载模型
    val model = ALSModel.load("model/Type/categoryFavorModel")

    val predicttoStringFunc = udf(predicttoString _)

    val result: DataFrame = model.recommendForAllUsers(10)
      .withColumn("favorTypes", predicttoStringFunc('recommendations))
      .withColumnRenamed("userId", "id")
      .drop('recommendations)
      .select('id.cast(LongType), 'favorTypes)

    //    result.show(20, false)

    val separator = ","
    lazy val first = result.first()

    val numAttrs = first.toString().split(separator).length
    val attrs = Array.tabulate(numAttrs)(n => "favorType_" + n)
    //按指定分隔符拆分value列，生成splitCols列
    var recommendationDF = result.withColumn("splitCols", split($"favorTypes", separator))
    attrs.zipWithIndex.foreach(x => {
      recommendationDF = recommendationDF.withColumn(x._1, $"splitCols".getItem(x._2))
    })
    val tempDF1 = recommendationDF
      .drop("favorTypes")
      .drop("splitCols")
      .drop("favorType_10")

    tempDF1.toDF().createOrReplaceTempView("tempTBL")

    //    tempDF1.show(20, false)

    val tempDF2 = tempDF1.select(
      'id,
      when('favorType_0 === 1, "电热水器")
        .when('favorType_0 === 2, "燃气灶")
        .when('favorType_0 === 3, "智能电视")
        .when('favorType_0 === 4, "微波炉")
        .when('favorType_0 === 5, "Haier/海尔冰箱")
        .when('favorType_0 === 6, "4K电视")
        .when('favorType_0 === 7, "烟灶套系")
        .when('favorType_0 === 8, "空气净化器")
        .when('favorType_0 === 9, "净水机")
        .when('favorType_0 === 10, "滤芯")
        .when('favorType_0 === 11, "电饭煲")
        .when('favorType_0 === 12, "料理机")
        .when('favorType_0 === 13, "吸尘器/除螨仪")
        .when('favorType_0 === 14, "电磁炉")
        .when('favorType_0 === 15, "前置过滤器")
        .when('favorType_0 === 16, "电水壶/热水瓶")
        .when('favorType_0 === 17, "LED电视")
        .when('favorType_0 === 18, "取暖电器")
        .when('favorType_0 === 19, "烤箱")
        .when('favorType_0 === 20, "嵌入式厨电")
        .otherwise("其他").as('favorType_0),
      when('favorType_1 === 1, "电热水器")
        .when('favorType_1 === 2, "燃气灶")
        .when('favorType_1 === 3, "智能电视")
        .when('favorType_1 === 4, "微波炉")
        .when('favorType_1 === 5, "Haier/海尔冰箱")
        .when('favorType_1 === 6, "4K电视")
        .when('favorType_1 === 7, "烟灶套系")
        .when('favorType_1 === 8, "空气净化器")
        .when('favorType_1 === 9, "净水机")
        .when('favorType_1 === 10, "滤芯")
        .when('favorType_1 === 11, "电饭煲")
        .when('favorType_1 === 12, "料理机")
        .when('favorType_1 === 13, "吸尘器/除螨仪")
        .when('favorType_1 === 14, "电磁炉")
        .when('favorType_1 === 15, "前置过滤器")
        .when('favorType_1 === 16, "电水壶/热水瓶")
        .when('favorType_1 === 17, "LED电视")
        .when('favorType_1 === 18, "取暖电器")
        .when('favorType_1 === 19, "烤箱")
        .when('favorType_1 === 20, "嵌入式厨电")
        .otherwise("其他").as('favorType_1),
      when('favorType_2 === 1, "电热水器")
        .when('favorType_2 === 2, "燃气灶")
        .when('favorType_2 === 3, "智能电视")
        .when('favorType_2 === 4, "微波炉")
        .when('favorType_2 === 5, "Haier/海尔冰箱")
        .when('favorType_2 === 6, "4K电视")
        .when('favorType_2 === 7, "烟灶套系")
        .when('favorType_2 === 8, "空气净化器")
        .when('favorType_2 === 9, "净水机")
        .when('favorType_2 === 10, "滤芯")
        .when('favorType_2 === 11, "电饭煲")
        .when('favorType_2 === 12, "料理机")
        .when('favorType_2 === 13, "吸尘器/除螨仪")
        .when('favorType_2 === 14, "电磁炉")
        .when('favorType_2 === 15, "前置过滤器")
        .when('favorType_2 === 16, "电水壶/热水瓶")
        .when('favorType_2 === 17, "LED电视")
        .when('favorType_2 === 18, "取暖电器")
        .when('favorType_2 === 19, "烤箱")
        .when('favorType_2 === 20, "嵌入式厨电")
        .otherwise("其他").as('favorType_2),
      when('favorType_3 === 1, "电热水器")
        .when('favorType_3 === 2, "燃气灶")
        .when('favorType_3 === 3, "智能电视")
        .when('favorType_3 === 4, "微波炉")
        .when('favorType_3 === 5, "Haier/海尔冰箱")
        .when('favorType_3 === 6, "4K电视")
        .when('favorType_3 === 7, "烟灶套系")
        .when('favorType_3 === 8, "空气净化器")
        .when('favorType_3 === 9, "净水机")
        .when('favorType_3 === 10, "滤芯")
        .when('favorType_3 === 11, "电饭煲")
        .when('favorType_3 === 12, "料理机")
        .when('favorType_3 === 13, "吸尘器/除螨仪")
        .when('favorType_3 === 14, "电磁炉")
        .when('favorType_3 === 15, "前置过滤器")
        .when('favorType_3 === 16, "电水壶/热水瓶")
        .when('favorType_3 === 17, "LED电视")
        .when('favorType_3 === 18, "取暖电器")
        .when('favorType_3 === 19, "烤箱")
        .when('favorType_3 === 20, "嵌入式厨电")
        .otherwise("其他").as('favorType_3),
      when('favorType_4 === 1, "电热水器")
        .when('favorType_4 === 2, "燃气灶")
        .when('favorType_4 === 3, "智能电视")
        .when('favorType_4 === 4, "微波炉")
        .when('favorType_4 === 5, "Haier/海尔冰箱")
        .when('favorType_4 === 6, "4K电视")
        .when('favorType_4 === 7, "烟灶套系")
        .when('favorType_4 === 8, "空气净化器")
        .when('favorType_4 === 9, "净水机")
        .when('favorType_4 === 10, "滤芯")
        .when('favorType_4 === 11, "电饭煲")
        .when('favorType_4 === 12, "料理机")
        .when('favorType_4 === 13, "吸尘器/除螨仪")
        .when('favorType_4 === 14, "电磁炉")
        .when('favorType_4 === 15, "前置过滤器")
        .when('favorType_4 === 16, "电水壶/热水瓶")
        .when('favorType_4 === 17, "LED电视")
        .when('favorType_4 === 18, "取暖电器")
        .when('favorType_4 === 19, "烤箱")
        .when('favorType_4 === 20, "嵌入式厨电")
        .otherwise("其他").as('favorType_4),
      when('favorType_5 === 1, "电热水器")
        .when('favorType_5 === 2, "燃气灶")
        .when('favorType_5 === 3, "智能电视")
        .when('favorType_5 === 4, "微波炉")
        .when('favorType_5 === 5, "Haier/海尔冰箱")
        .when('favorType_5 === 6, "4K电视")
        .when('favorType_5 === 7, "烟灶套系")
        .when('favorType_5 === 8, "空气净化器")
        .when('favorType_5 === 9, "净水机")
        .when('favorType_5 === 10, "滤芯")
        .when('favorType_5 === 11, "电饭煲")
        .when('favorType_5 === 12, "料理机")
        .when('favorType_5 === 13, "吸尘器/除螨仪")
        .when('favorType_5 === 14, "电磁炉")
        .when('favorType_5 === 15, "前置过滤器")
        .when('favorType_5 === 16, "电水壶/热水瓶")
        .when('favorType_5 === 17, "LED电视")
        .when('favorType_5 === 18, "取暖电器")
        .when('favorType_5 === 19, "烤箱")
        .when('favorType_5 === 20, "嵌入式厨电")
        .otherwise("其他").as('favorType_5),
      when('favorType_6 === 1, "电热水器")
        .when('favorType_6 === 2, "燃气灶")
        .when('favorType_6 === 3, "智能电视")
        .when('favorType_6 === 4, "微波炉")
        .when('favorType_6 === 5, "Haier/海尔冰箱")
        .when('favorType_6 === 6, "4K电视")
        .when('favorType_6 === 7, "烟灶套系")
        .when('favorType_6 === 8, "空气净化器")
        .when('favorType_6 === 9, "净水机")
        .when('favorType_6 === 10, "滤芯")
        .when('favorType_6 === 11, "电饭煲")
        .when('favorType_6 === 12, "料理机")
        .when('favorType_6 === 13, "吸尘器/除螨仪")
        .when('favorType_6 === 14, "电磁炉")
        .when('favorType_6 === 15, "前置过滤器")
        .when('favorType_6 === 16, "电水壶/热水瓶")
        .when('favorType_6 === 17, "LED电视")
        .when('favorType_6 === 18, "取暖电器")
        .when('favorType_6 === 19, "烤箱")
        .when('favorType_6 === 20, "嵌入式厨电")
        .otherwise("其他").as('favorType_6),
      when('favorType_7 === 1, "电热水器")
        .when('favorType_7 === 2, "燃气灶")
        .when('favorType_7 === 3, "智能电视")
        .when('favorType_7 === 4, "微波炉")
        .when('favorType_7 === 5, "Haier/海尔冰箱")
        .when('favorType_7 === 6, "4K电视")
        .when('favorType_7 === 7, "烟灶套系")
        .when('favorType_7 === 8, "空气净化器")
        .when('favorType_7 === 9, "净水机")
        .when('favorType_7 === 10, "滤芯")
        .when('favorType_7 === 11, "电饭煲")
        .when('favorType_7 === 12, "料理机")
        .when('favorType_7 === 13, "吸尘器/除螨仪")
        .when('favorType_7 === 14, "电磁炉")
        .when('favorType_7 === 15, "前置过滤器")
        .when('favorType_7 === 16, "电水壶/热水瓶")
        .when('favorType_7 === 17, "LED电视")
        .when('favorType_7 === 18, "取暖电器")
        .when('favorType_7 === 19, "烤箱")
        .when('favorType_7 === 20, "嵌入式厨电")
        .otherwise("其他").as('favorType_7),
      when('favorType_8 === 1, "电热水器")
        .when('favorType_8 === 2, "燃气灶")
        .when('favorType_8 === 3, "智能电视")
        .when('favorType_8 === 4, "微波炉")
        .when('favorType_8 === 5, "Haier/海尔冰箱")
        .when('favorType_8 === 6, "4K电视")
        .when('favorType_8 === 7, "烟灶套系")
        .when('favorType_8 === 8, "空气净化器")
        .when('favorType_8 === 9, "净水机")
        .when('favorType_8 === 10, "滤芯")
        .when('favorType_8 === 11, "电饭煲")
        .when('favorType_8 === 12, "料理机")
        .when('favorType_8 === 13, "吸尘器/除螨仪")
        .when('favorType_8 === 14, "电磁炉")
        .when('favorType_8 === 15, "前置过滤器")
        .when('favorType_8 === 16, "电水壶/热水瓶")
        .when('favorType_8 === 17, "LED电视")
        .when('favorType_8 === 18, "取暖电器")
        .when('favorType_8 === 19, "烤箱")
        .when('favorType_8 === 20, "嵌入式厨电")
        .otherwise("其他").as('favorType_8),
      when('favorType_9 === 1, "电热水器")
        .when('favorType_9 === 2, "燃气灶")
        .when('favorType_9 === 3, "智能电视")
        .when('favorType_9 === 4, "微波炉")
        .when('favorType_9 === 5, "Haier/海尔冰箱")
        .when('favorType_9 === 6, "4K电视")
        .when('favorType_9 === 7, "烟灶套系")
        .when('favorType_9 === 8, "空气净化器")
        .when('favorType_9 === 9, "净水机")
        .when('favorType_9 === 10, "滤芯")
        .when('favorType_9 === 11, "电饭煲")
        .when('favorType_9 === 12, "料理机")
        .when('favorType_9 === 13, "吸尘器/除螨仪")
        .when('favorType_9 === 14, "电磁炉")
        .when('favorType_9 === 15, "前置过滤器")
        .when('favorType_9 === 16, "电水壶/热水瓶")
        .when('favorType_9 === 17, "LED电视")
        .when('favorType_9 === 18, "取暖电器")
        .when('favorType_9 === 19, "烤箱")
        .when('favorType_9 === 20, "嵌入式厨电")
        .otherwise("其他").as('favorType_9)
    )

    tempDF2.show(100, false)

    def recommendationCatalog =
      s"""{
         |  "table":{"namespace":"default", "name":"user_profile"},
         |  "rowkey":"id",
         |   "columns":{
         |     "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |     "favorType_0":{"cf":"cf", "col":"favorType_0", "type":"string"},
         |     "favorType_1":{"cf":"cf", "col":"favorType_1", "type":"string"},
         |     "favorType_2":{"cf":"cf", "col":"favorType_2", "type":"string"},
         |     "favorType_3":{"cf":"cf", "col":"favorType_3", "type":"string"},
         |     "favorType_4":{"cf":"cf", "col":"favorType_4", "type":"string"},
         |     "favorType_5":{"cf":"cf", "col":"favorType_5", "type":"string"},
         |     "favorType_6":{"cf":"cf", "col":"favorType_6", "type":"string"},
         |     "favorType_7":{"cf":"cf", "col":"favorType_7", "type":"string"},
         |     "favorType_8":{"cf":"cf", "col":"favorType_8", "type":"string"},
         |     "favorType_9":{"cf":"cf", "col":"favorType_9", "type":"string"}
         |   }
         |}""".stripMargin

    //    tempDF2.write
    //      .option(HBaseTableCatalog.tableCatalog, recommendationCatalog)
    //      .option(HBaseTableCatalog.newTable, "5")
    //      .format("org.apache.spark.sql.execution.datasources.hbase")
    //      .save()
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
    arr.map(_.getAs[Int]("productType")).mkString(",")
  }
}
