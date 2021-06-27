package consumption
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

//消费特征：客服咨询频率

object consultationFrequency {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("consultationFrequency")
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
    tempDF = spark.sql("select id,count(if(orderStatus!='204',1,null)) `noconsultation`,count(if(orderStatus='204',1,null)) `consultation`,count(*) `count` from temp group by id ")
    tempDF = tempDF.select('id,('consultation/'count) as "consultationFrequency")
//    tempDF.show(950)

//    统计计算平均咨询率 0.0003
//    tempDF.select(sum('consultationFrequency).cast("double")).show()
    /*
+------------------------------------------+
|CAST(sum(consultationFrequency) AS DOUBLE)|
+------------------------------------------+
|                       0.18214576091082033|
+------------------------------------------+
     */
//    println(tempDF.count())
    /*
    543
     */

    val result = tempDF.select('id,
      when('consultationFrequency >= 0.02,"高")
        .when('consultationFrequency >= 0.002,"中")
        .otherwise("低")
        .as("consultationFrequency"))
//    result.show()


//    写入mysql
    result
      .write.format("jdbc").mode(SaveMode.Overwrite)
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_consultationFrequency")
      .option("user","root")
      .option("password","mysqlroot")
      .save()

//    查看mysql数据
    spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
      .option("dbtable","up_consultationFrequency")
      .option("user","root")
      .option("password","mysqlroot")
      .load()
      .show()

    spark.stop()
  }

}
