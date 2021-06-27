package behaviour

import org.apache.spark.sql.SparkSession

/**
  * @author Tracy
  * @date 2021/6/24 10:02
  */
object PurchaseGoods {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("purchaseGoods")
      .master("local")
      .getOrCreate()

    //    读数据
    def catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_logs"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"global_user_id":{"cf":"cf","col":"global_user_id","type":"string"},
        |"user_agent":{"cf":"cf","col":"user_agent","type":"string"}
        |}}
      """.stripMargin
  }
}
