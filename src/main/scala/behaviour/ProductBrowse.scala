package behaviour

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object ProductBrowse {
  def main(args: Array[String]): Unit = {
    def catalog1 =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_logs"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"global_user_id":{"cf":"cf", "col":"global_user_id", "type":"string"},
         |"log_time":{"cf":"cf", "col":"log_time", "type":"string"},
         |"loc_url":{"cf":"cf", "col":"loc_url", "type":"string"},
         |"ref_url":{"cf":"cf", "col":"ref_url", "type":"string"}
         |}
         |}""".stripMargin

    def catalog2 =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_goods"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"productId":{"cf":"cf", "col":"productId", "type":"string"},
         |"productName":{"cf":"cf", "col":"productName", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._



    val readDF_logs: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog1)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .toDF()

    val readDF_goods: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog2)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .toDF()

    var pageBrowsePre1: DataFrame = readDF_logs
      .select('global_user_id.cast("int").as("id"),'log_time,
        when('loc_url.like("%/itemlist/%"), 'loc_url)
          .when('loc_url.like("%/item/%"), 'loc_url)
          .when('loc_url.like("%/product/%"), 'loc_url)
          .when('loc_url.like("%/productSpecification/%"), 'loc_url)
          .otherwise(0)
          .as("loc_status")
      )
      .sort('id)

    var pageBrowsePre2: DataFrame = readDF_logs
      .select('global_user_id.cast("int").as("id"),'log_time,
        when('ref_url.like("%/itemlist/%"), 'loc_url)
          .when('ref_url.like("%/item/%"), 'loc_url)
          .when('ref_url.like("%/product/%"), 'loc_url)
          .when('ref_url.like("%/productSpecification/%"), 'loc_url)
          .otherwise(0)
          .as("ref_status")
        //如果有满足要求的，则这里一列将会填上目标url，否则为空

      )
      .sort('id.asc)

   //筛选非空的列值，然后对列值做split操作，将productId取出，然后join tbl_goods中的productId进行后续处理
    var pageBrowse1 = pageBrowsePre1
      .where('loc_status =!= "0")
      .select('id,'log_time, split(reverse(split('loc_status, "\\/"))(0), "\\.html")(0).as("splitresult"))
    var pageBrowse2 = pageBrowsePre2
      .where('ref_status =!= "0")
      .select('id,'log_time, split(reverse(split('ref_status, "\\/"))(0), "\\.html")(0).as("splitresult"))

    pageBrowse1.select('id,to_timestamp('log_time,"yyyy-MM-dd HH:mm:ss") as "log_time",'splitresult).createOrReplaceTempView("p1")
    pageBrowse1 = spark.sql("select p1.id,p1.splitresult from p1 join (select id,max(log_time) `log_time` from p1 group by id) p on p1.id = p.id where p1.log_time = p.log_time")

    pageBrowse2.select('id,to_timestamp('log_time,"yyyy-MM-dd HH:mm:ss") as "log_time",'splitresult).createOrReplaceTempView("p2")
    pageBrowse2 = spark.sql("select p2.id,p2.splitresult from p2 join (select id,max(log_time) `log_time` from p2 group by id) p on p2.id = p.id where p2.log_time = p.log_time")



    val result1 = pageBrowse1.join(readDF_goods, pageBrowse1.col("splitresult") === readDF_goods.col("productId"))
      .select(pageBrowse1.col("id"),
        pageBrowse1.col("splitresult"),
        readDF_goods.col("productId"),
        readDF_goods.col("productName"))

    val result2 = result1
      .repartition('productId)
      .sortWithinPartitions('productId, length('productName).desc)
      .toDF()

    val result3 = result2
      .groupBy("id", "productId")
      .agg(collect_list("productName").as("combined_list"))
      //去掉大括号以便处理
      .withColumn("combined_list", concat_ws(",", col("combined_list")))
      .select('id,
        split('combined_list, "\\,")(0).as("productName"))
      .sort('id.asc)
//    result3.show(false)

    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"productName":{"cf":"cf","col":"productName","type":"string"}
        |}}
      """.stripMargin
    result3
      .where('id <= 950)
      .select('id.cast("string") as "id",'productName)
      .write
      .option(HBaseTableCatalog.tableCatalog,catalogwrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()


//    result3
//      .write.format("jdbc").mode(SaveMode.Overwrite)
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_ProductBrowse")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .save()
//
//    //查看mysql数据
//    spark.read
//      .format("jdbc")
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_ProductBrowse")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .load()
//      .show()

    spark.stop()
  }
}
