package consumption

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

//消费特征：支付方式

object paymentMode {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "paymentCode":{"cf":"cf", "col":"paymentCode", "type":"string"}
         |  }
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    source.show(20, false)


    //计算所有订单支付最多的方式
    val result = source.groupBy('memberId, 'paymentCode)
      .agg(count('paymentCode) as "count")
      .withColumn("row_num", row_number() over Window.partitionBy('memberId).orderBy('count.desc))
      .where('row_num === 1)
      .withColumnRenamed("memberId", "id").drop("count", "row_num")
//      提取会员id后三位、更新支付方式名称
      .select('id.cast("int").mod(1000) as "id",
        when('paymentCode === "alipay","支付宝")
          .when('paymentCode === "wxpay","微信")
          .when('paymentCode === "balance","余额")
          .when(('paymentCode==="ccb") or ('paymentCode==="chinaecpay") or ('paymentCode==="chinapay"),"银行卡")
          .otherwise("其他方式")
          .as("paymentCode")
      )

//    result.show(950, false)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |  "paymentCode":{"cf":"cf", "col":"paymentCode", "type":"string"}
         |}
         |}""".stripMargin

//    result.write
//      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
//      .option(HBaseTableCatalog.newTable, "5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save



//    写入mysql
    result
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_paymentCode")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

//    查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_paymentCode")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show()





    spark.stop()
  }
}
