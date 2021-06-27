package behaviour

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, length, reverse, split, when}

object ProductPurchasing {
  def main(args: Array[String]): Unit = {
    def catalog_goods =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_goods"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"cOrderSn":{"cf":"cf", "col":"cOrderSn", "type":"string"},
         |"productName":{"cf":"cf", "col":"productName", "type":"string"},
         |"productId":{"cf":"cf", "col":"productId", "type":"string"},
         |"productType":{"cf":"cf", "col":"productType", "type":"string"}
         |}
         |}""".stripMargin

    def catalog_orders =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_orders"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"orderSn":{"cf":"cf", "col":"orderSn", "type":"string"},
         |"memberId":{"cf":"cf", "col":"memberId", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()


    import spark.implicits._

    val readDF_goods: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_goods)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .toDF()

    val readDF_orders: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_orders)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .toDF()
      .drop('id)
      //添加列名为user_id的列，也就是取memberid后三位
      .withColumn("id", col("memberId").substr(-3, 3).cast("int"))


    val result1 = readDF_orders.join(readDF_goods, readDF_orders.col("orderSn") === readDF_goods.col("cOrderSn"))
      .select(
        readDF_orders.col("id"),
        readDF_orders.col("orderSn").as("orderSn_orders"),
        readDF_goods.col("cOrderSn").as("orderSn_goods"),
        readDF_goods.col("productId"),
        readDF_goods.col("productName"),
        readDF_goods.col("productType")
      )
      .sort('id.asc)

    result1.show(false)
    val result2 = result1
      .repartition('productId)
      .sortWithinPartitions('productId, length('productName).desc)
      .toDF()

    val result3 = result2
      .groupBy("id", "productId")
      .agg(collect_list("productName").as("combined_list"))
      //去掉大括号以便处理
      .withColumn("combined_list", concat_ws(",", col("combined_list")))
      .select('id, 'productId, split('combined_list, "\\,")(0).as("productName"))
      .sort('id.asc)

    val result4 = result3
      .groupBy("id")
      .agg(collect_list("productName").as("combined_list"))
      .withColumn("productPurchased", concat_ws(",", col("combined_list")))
      .drop("combined_list")
      .sort('id.asc)
    //        result2.show(false)
    result4.show(false)

    result4
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_ProductPurchasing")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

    //查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_ProductPurchasing")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show()

    spark.stop()


  }
}

