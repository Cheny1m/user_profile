package behaviour

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._


object LoginFrequency {
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

    val readDF: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .toDF()

    var result1: DataFrame = readDF
      .select('global_user_id.cast("int").as("id"), 'loc_url, 'log_time,
        when('loc_url.like("%login%"), 1)
          .when('loc_url.like("%eshop.com/"), 1)
          .when('loc_url.like("%m.eshop.com/?source=mobile"), 1)
          .otherwise(0)
          .as("login_or_not")
      )
      .where('login_or_not =!= 0)

    result1.show()

    val result2 = result1.
      groupBy("id")
      .agg(sum("login_or_not").as("loginSum"))
      .sort('loginSum.asc)

    val result3 = result2.
      select('id
        , when('loginSum >= 80, "经常")
          .when('loginSum >= 65 && 'loginSum < 80, "一般")
          .when('loginSum >= 55 && 'loginSum < 65, "较少")
          .when('loginSum > 0 && 'loginSum < 55, "极少")
          .as("LoginFrequency")
      )
      .sort('id.asc)
      .where('id <= 950)
      .sort('id.asc)

    result3.show(false)


    result3
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_LoginFrequency")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

    //查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_LoginFrequency")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show()

    spark.stop()


  }
}

