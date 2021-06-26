package attribute

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._


//人口属性：政治面貌
object politicalFace {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("politicalFace")
      .master("local")
      .getOrCreate()

    import  spark.implicits._

//    读数据
    def catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_users"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"politicalFace":{"cf":"cf","col":"politicalFace","type":"string"}
        |}}
      """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
//    readDF.show()

//    数据处理
    val resDF = readDF.select('id,
        when('politicalFace === "1","群众").
        when('politicalFace === "2","党员").
        when('politicalFace === "3","无党派人士").
        otherwise("其他").
        as("politicalFace"))
//    resDF.show()


//    写入hase
    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"politicalFace":{"cf":"cf","col":"politicalFace","type":"string"}
        |}}
      """.stripMargin
    resDF.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

//    查看结果，需注释上方写操作
//    spark.read
//      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//      .show()


//    写入mysql
//    resDF.write.format("jdbc").mode(SaveMode.Overwrite)
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_politicalFace")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .save()
//
//    查看mysql数据
//    spark.read
//      .format("jdbc")
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_politicalFace")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .load()
//      .show()

    spark.stop()
  }

}
