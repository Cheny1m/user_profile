package consumption

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

object exchangeRate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("exchangeRate")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    //    读数据
    def catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_orders"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"memberId":{"cf":"cf","col":"memberId","type":"string"},
        |"orderStatus":{"cf":"cf","col":"orderStatus","type":"string"}
        |}}
      """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

//    提取会员号后三位、过滤取消订单、统计换货率
    var tempDF = readDF.select('memberId.cast("int").mod(1000) as "mid",'orderStatus)
      .where('orderStatus =!= "203")
      .select('mid as "id",'orderStatus)
    tempDF.createOrReplaceTempView("temp")
    tempDF = spark.sql("select id,count(if(orderStatus!='1',1,null)) `noexchange`,count(if(orderStatus='1',1,null)) `exchange`,count(*) `count` from temp group by id ")
    tempDF = tempDF.select('id,('exchange/'count) as "exchangeRate")
//    tempDF.show(950)

//    平均退货率,计算结果有问题，没有参考意义
//    tempDF.select(sum('exchangeRate).cast("double") / tempDF.count()).show()

//    得到换货率画像
    val result = tempDF.select('id,
      when('exchangeRate >= 0.01,"高")
        .when('exchangeRate >=0.0001,"中")
        .otherwise("低")
        .as("exchangeRate"))
//    result.show(950)

//    写入mysql
    result
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_exchangeRate")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

//    查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_exchangeRate")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show()



    spark.stop()
  }
}
