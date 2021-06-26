package consumption

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._


object consumptionCycle {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("consumptionCycle")
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
        |"finishTime":{"cf":"cf","col":"finishTime","type":"string"}
        |}}
      """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
//    readDF.show()

    readDF.select('memberId.cast("int") as "mid",'finishTime.cast("int")as "finishTime")
      .select('mid.mod(1000) as "id", 'finishTime)
      .createOrReplaceTempView("temp")
    val tempDF = spark.sql("select id,count(*) `count`,max(finishTime) `max`,min(finishTime) `min` from temp group by id")

    val result = tempDF.select('id,(('max - 'min) / 'count) as "interval")
      .select('id,
        when('interval >= 60*60*24*30*6,"6月")
          .when('interval >= 60*60*24*30*5,"5月")
          .when('interval >= 60*60*24*30*4,"4月")
          .when('interval >= 60*60*24*30*3,"3月")
          .when('interval >= 60*60*24*30*2,"2月")
          .when('interval >= 60*60*24*30,"1月")
          .when('interval >= 60*60*24*14,"2周")
          .when('interval >= 60*60*24*7,"7日")
          .when('interval >= 60*60*24*5,"5日")
          .when('interval >= 60*60*24*3,"3日")
          .when('interval >= 60*60*24*2,"2日")
          .otherwise("1日")
          .as("consumptionCycle"))
//    result.show(950)

//    写入mysql
    result.write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_consumptionCycle")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

//    查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_consumptionCycle")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show()




    spark.stop()
  }

}
