package behaviour


import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._


object BrowseFrequency {
  def main(args: Array[String]): Unit = {
    //订单ID 用户ID 浏览网页地址
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"loc_url":{"cf":"cf", "col":"loc_url", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .toDF()

    //对于浏览的网页进行计数
    var result1: DataFrame = readDF
      .select('global_user_id.cast("int").as("id"), 'loc_url,
        when('loc_url.like("%login%"), 1)
          .when('loc_url.like("%eshop.com/"), 1)
          .when('loc_url.like("%m.eshop.com/?source=mobile"), 1)
          .when('loc_url.like("%/itemlist/%"), 1)
          .when('loc_url.like("%/l/%"), 1)
          .when('loc_url.like("%/item/%"), 1)
          .when('loc_url.like("%/product/%"), 1)
          .when('loc_url.like("%/order/%"), 1)
          .when('loc_url.like("%cart%"), 1)
          .when('loc_url.like("%Cart%"), 1)
          .otherwise(0)
          .as("Browse_or_not")
      )
      .where('Browse_or_not =!= 0)

    result1.show()


    //浏览次数
    val result2 = result1.
      groupBy("id")
      .agg(sum("Browse_or_not").as("BrowseSum"))
      .sort('BrowseSum.desc)

    //统计匹配
    val result3 = result2.
      select('id
        , when('BrowseSum >= 255, "经常")
          .when('BrowseSum >=230  && 'BrowseSum < 255, "较常")
          .when('BrowseSum >= 200 && 'BrowseSum < 230, "偶尔")
          .when('BrowseSum<200 && 'BrowseSum>=0, "很少")
          .as("browseFrequency")
      )
      .sort('id.asc)
      //对用户进行筛选，直接剔除大于950的
      .where('id <= 950)
      .sort('id.asc)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |  "browseFrequency":{"cf":"cf", "col":"browseFrequency", "type":"string"}
         |}
         |}""".stripMargin

    result3
//      .where('id<=950)
      .select('id.cast("string") as "id",'browseFrequency)
      .write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()


//    result3.show(false)
//
//    result3
//      .write.format("jdbc").mode(SaveMode.Overwrite)
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_BrowseFrequency")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .save()

    //查看mysql数据
//    spark.read
//      .format("jdbc")
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_BrowseFrequency")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .load()
//      .show()

    spark.stop()


  }
}

