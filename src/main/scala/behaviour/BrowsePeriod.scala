package behaviour

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{col, hour, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BrowsePeriod {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"}
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
      .withColumn("Browse_time", hour('log_time).cast("long"))
      .drop('log_time)
      .drop('id)
      .select('global_user_id.as("id").cast("long"),
        when('Browse_time.geq(1).and('Browse_time.leq(7)), 1).otherwise(0).as("1-7"),
        when('Browse_time.geq(8).and('Browse_time.leq(12)), 1).otherwise(0).as("8-12"),
        when('Browse_time.geq(13).and('Browse_time.leq(17)), 1).otherwise(0).as("13-17"),
        when('Browse_time.geq(18).and('Browse_time.leq(21)), 1).otherwise(0).as("18-21"),
        when('Browse_time.geq(22).and('Browse_time.leq(24)), 1).otherwise(0).as("22-24")
      )
      .groupBy('id)
      .sum("1-7", "8-12", "13-17", "18-21", "22-24")
      .withColumnRenamed("sum(1-7)", "1-7")
      .withColumnRenamed("sum(8-12)", "8-12")
      .withColumnRenamed("sum(13-17)", "13-17")
      .withColumnRenamed("sum(18-21)", "18-21")
      .withColumnRenamed("sum(22-24)", "22-24")
      .sort('id)

    val df1: DataFrame = df
      .withColumn("MAXBrowsePeriod",
        when(
          col("1-7").geq(col("8-12"))
            .and(col("1-7").geq(col("13-17")))
            .and(col("1-7").geq(col("18-21")))
            .and(col("1-7").geq(col("22-24"))), "1-7")
          .when(
            col("8-12").geq(col("1-7"))
              .and(col("8-12").geq(col("13-17")))
              .and(col("8-12").geq(col("18-21")))
              .and(col("8-12").geq(col("22-24"))), "8-12")
          .when(
            col("13-17").geq(col("8-12"))
              .and(col("13-17").geq(col("1-7")))
              .and(col("13-17").geq(col("18-21")))
              .and(col("13-17").geq(col("22-24"))), "13-17")
          .when(
            col("18-21").geq(col("8-12"))
              .and(col("18-21").geq(col("13-17")))
              .and(col("18-21").geq(col("1-7")))
              .and(col("18-21").geq(col("22-24"))), "18-21")
          .when(
            col("22-24").geq(col("8-12"))
              .and(col("22-24").geq(col("13-17")))
              .and(col("22-24").geq(col("18-21")))
              .and(col("22-24").geq(col("1-7"))), "22-24")

      )

//    val df2: DataFrame = df1
//      .withColumnRenamed("1-7", "1点-7点")
//      .withColumnRenamed("8-12", "8点-12点")
//      .withColumnRenamed("13-17", "13点-17点")
//      .withColumnRenamed("18-21", "18点-21点")
//      .withColumnRenamed("22-24", "22点-24点")

    //df1.show(200, false)

    val df3:DataFrame = df1.select('id,'MAXBrowsePeriod as "browsePeriod")
//    df3.show(200, false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |  "browsePeriod":{"cf":"cf", "col":"browsePeriod", "type":"string"}
         |}
         |}""".stripMargin
    df3
      .where('id<=950)
      .select('id.cast("string") as "id",'browsePeriod)
      .write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()



//    df3
//      .write.format("jdbc").mode(SaveMode.Overwrite)
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_BrowsePeriod")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .save()

    //查看mysql数据
//    spark.read
//      .format("jdbc")
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_BrowsePeriod")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .load()
//      .show(200)

    spark.stop()
  }
}
