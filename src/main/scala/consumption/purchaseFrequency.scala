package consumption

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

//消费特征：购买频率

object purchaseFrequency {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("purchaseFrequency")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    def catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_orders"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"memberId":{"cf":"cf","col":"memberId","type":"string"}
        |}}
      """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

//    提取会员号后三位
    var tempDF = readDF.select('memberId.cast("int").mod(1000) as "mid")
      .select('mid as "id")
    tempDF.createOrReplaceTempView("temp")
//    统计次数
    tempDF = spark.sql("select id,count(*) `count` from temp group by id")
//    tempDF.show()

//    查看购买次数平均值
//    tempDF.select(sum('count) / tempDF.count()).show()
    /*
    +------------------+
    |(sum(count) / 543)|
    +------------------+
    |221.22467771639043|
    +------------------+
     */

//    得到频率
    val result = tempDF.select('id,
        when('count >= 250,"高")
          .when('count >= 200,"中")
          .otherwise("低")
          .as("purchaseFrequency"))
//    result.show(950)


//    写入mysql
    result
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_purchaseFrequency")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

//    查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_purchaseFrequency")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show()


    spark.stop()
  }

}
