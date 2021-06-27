package behaviour

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._


object PageBrowse {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"},
         |"loc_url":{"cf":"cf", "col":"loc_url", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .toDF()

    //登录页:1
    //首页:2
    //分类页:3
    //商品页:4
    //订单页:5
    //购物车页:6
    //其他页面：0

    var result: DataFrame = df
      .select('global_user_id.cast("int").as("id"), 'loc_url, 'log_time,
        when('loc_url.like("%login%"), 1).otherwise(0)
          .as("登录页"),
        when('loc_url.like("%eshop.com/"), 1)
          .when('loc_url.like("%m.eshop.com/?source=mobile"), 1).otherwise(0)
          .as("首页"),
        when('loc_url.like("%/itemlist/%"), 1)
          .when('loc_url.like("%/l/%"), 1).otherwise(0)
          .as("分类页"),
        when('loc_url.like("%/item/%"), 1)
          .when('loc_url.like("%/product/%"), 1).otherwise(0)
          .as("商品页"),
        when('loc_url.like("%/order/%"), 1).otherwise(0)
          .as("订单页")
      )
      .groupBy('id)
      .sum("登录页", "首页", "分类页", "商品页", "订单页"
        //,"购物车页"
      )
      .sort('id)

    result.show(false)


    result
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_PageBrowse")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

    //查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_PageBrowse")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show()

    spark.stop()
  }
}
