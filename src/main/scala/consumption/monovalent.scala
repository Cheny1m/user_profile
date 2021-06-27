package consumption

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

//消费特征：客单价

object monovalent {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("monovalent")
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
        |"orderAmount":{"cf":"cf","col":"orderAmount","type":"string"},
        |"orderStatus":{"cf":"cf","col":"orderStatus","type":"string"}
        |}}
      """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
//        readDF.show()

//    提取会员号后三位、只保留完成订单、订单金额类型转换
    val tempDF = readDF.select('memberId.cast("int").mod(1000) as "mid",'orderAmount.cast("double") as "orderAmount",'orderStatus)
      .where('orderStatus === "202")
      .select('mid as "id",'orderAmount)
    tempDF.createOrReplaceTempView("temp")
//    用sql计算客单价
    val result = spark.sql("select id,avg(orderAmount) `monovalent` from temp group by id")
//    result.show()


//    写入mysql
    result
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_monovalent")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

//    查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_monovalent")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show()

    spark.stop()
  }

}
