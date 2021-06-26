package consumption

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

//消费特征：单笔最高

object maxAmount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("maxAmount")
      .master("local")
      .getOrCreate()

    import  spark.implicits._


    def catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_orders"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"memberId":{"cf":"cf","col":"memberId","type":"string"},
        |"orderAmount":{"cf":"cf","col":"orderAmount","type":"string"},
        |"orderStatus":{"cf":"cf","col":"orderStatus","type":"string"}
        |}}
      """.stripMargin
//    读数据
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .option(HBaseTableCatalog.newTable,"5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

//    取会员id后三位、只保留完成订单、订单金额转int
    readDF.select('memberId.cast("int").mod(1000) as "mid",'orderAmount,'orderStatus)
      .where('orderStatus === "202")
      .select('mid as "id",'orderAmount.cast("int") as "orderAmount")
      .createOrReplaceTempView("temp")

//  查询每个id最大金额
    val tempDF = spark.sql("select id,max(orderAmount) `maxAmount` from temp group by id")


//    readDF.show()

//    数据处理
    val result = tempDF.select('id,'maxAmount as "amount")
      .select('id,
        when('amount >= 50000 ,"50000及以上")
        .when('amount >= 20000,"20000-49999")
        .when('amount >= 10000,"10000-19999")
        .when('amount >= 5000, "5000-9999")
        .when('amount >= 1, "1-4999")
        .otherwise("未消费")
        .as("maxAmount"))
//    result.show()

//    写入hbase
    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"use_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"maxAmount":{"cf":"cf","col":"maxAmount","type":"string"}
        |}}
      """.stripMargin
//    result.write
//      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
//      .option(HBaseTableCatalog.newTable,"5")
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()

//    查看hbase写入结果，要先注释上面写操作
//    spark.read
//      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//      .show(950,false)

//    写入mysql
    result
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_maxAmount")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

//    查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_maxAmount")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show()

    spark.stop()
  }

}
