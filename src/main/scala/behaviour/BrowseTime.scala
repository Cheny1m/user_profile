package behaviour

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

/**
  * @author Tracy
  * @date 2021/6/23 10:22
  */

//行为特征：浏览时长

object BrowseTime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("browseTime")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    //    读数据
    def catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_logs"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"global_user_id":{"cf":"cf","col":"global_user_id","type":"string"},
        |"log_time":{"cf":"cf","col":"log_time","type":"string"}
        |}}
      """.stripMargin

    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
//    readDF.show(20)

//    最近一天的最后登录时间减去上一个登陆时间作为浏览时间
    var tempDF = readDF.select('global_user_id.cast("int") as "id",to_date('log_time,"yyyy-MM-dd HH:mm:ss") as 'date,to_timestamp('log_time,"yyyy-MM-dd HH:mm:ss").cast("int") as 'timestamp)
    tempDF.createOrReplaceTempView("temp")
    tempDF = spark.sql("select temp.id,temp.timestamp from temp join (select id,max(date) `date` from temp group by id ) t on temp.id = t.id where temp.date= t.date")
    tempDF.createOrReplaceTempView("temp2")
    tempDF = spark.sql("select t1.id,t2.max2-max(t1.timestamp) `time` from temp2 t1 join (select id,max(timestamp) `max2` from temp2 group by id) t2 on t1.id=t2.id where t1.timestamp<t2.max2 group by t1.id,t2.max2")
//    tempDF.show()

    val result = tempDF.select('id,
      when('time > 300,"5分钟以上")
        .when('time > 60,"1-5分钟")
        .otherwise("1分钟内")
        .as("browseTime"))

//    result.show(950)



    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |  "browseTime":{"cf":"cf", "col":"browseTime", "type":"string"}
         |}
         |}""".stripMargin
    result
      .where('id<=950)
      .select('id.cast("string") as "id",'browseTime)
      .write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()




    //    写入mysql
//    result
//      .write.format("jdbc").mode(SaveMode.Overwrite)
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_browserTime")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .save()

    //    查看mysql数据
//    spark.read
//      .format("jdbc")
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_browserTime")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .load()
//      .show(950)

    spark.stop()
  }
}
