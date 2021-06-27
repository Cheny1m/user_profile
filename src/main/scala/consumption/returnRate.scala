package consumption

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

//消费特征：退货率
object returnRate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("returnRate")
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

//    提取会员号后三位、筛除取消订单、统计退货率
    var tempDF = readDF.select('memberId.cast("int").mod(1000) as "mid",'orderStatus)
      .where('orderStatus =!= "203")
      .select('mid as "id",'orderStatus)
    tempDF.createOrReplaceTempView("temp")
    tempDF = spark.sql("select id,count(if(orderStatus!='0',1,null)) `noreturn`,count(if(orderStatus='0',1,null)) `return`,count(*) `count` from temp group by id ")
    tempDF = tempDF.select('id,('return/'count) as "returnRate")
//    tempDF.select(sum('returnRate) / tempDF.count()).show()
/*  平均退货率
+-----------------------+
|(sum(returnRate) / 543)|
+-----------------------+
|   0.005898049461874867|
+-----------------------+
 */

//    得到退货率画像
    val result = tempDF.select('id,
      when('returnRate >= 0.02,"高")
        .when('returnRate >=0.0002,"中")
        .otherwise("低")
        .as("returnRate"))
//    result.show(950)


//    写入mysql
    result
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_returnRate")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

//    查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_returnRate")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show()


    spark.stop()
  }
}
