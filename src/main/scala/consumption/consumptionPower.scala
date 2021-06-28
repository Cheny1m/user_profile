package consumption

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.when

object consumptionPower {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("consumptionPower")
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

    //    提取会员号后三位、只保留完成订单、订单金额类型转换
    var tempDF = readDF.select('memberId.cast("int").mod(1000) as "mid",'orderAmount.cast("double") as "orderAmount",'orderStatus)
      .where('orderStatus === "202")
      .select('mid as "id",'orderAmount)
    tempDF.createOrReplaceTempView("temp")
    //    用sql计算消费金额之和
    tempDF = spark.sql("select id,sum(orderAmount) `sumAmount` from temp group by id order by sumAmount desc")
//    tempDF.show()
    val result = tempDF.where('id<=950)
      .select('id.cast("string") as "id",
        when('sumAmount >= 500000,"超高")
          .when('sumAmount >= 200000,"高")
          .when('sumAmount >= 150000,"中上")
          .when('sumAmount >= 100000,"中")
          .when('sumAmount >= 50000,"中下")
          .when('sumAmount >= 10000,"低")
          .otherwise("很低")
          .as("consumptionPower"))

//    result.show(950)

    //    写入hbase
    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"consumptionPower":{"cf":"cf","col":"consumptionPower","type":"string"}
        |}}
      """.stripMargin
    result.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()



    spark.stop()
  }
}
