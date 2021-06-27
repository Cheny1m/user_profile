package attribute

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

//人口属性：性别

object gender {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("gender")
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
         |"gender":{"cf":"cf","col":"gender","type":"string"}
         |}}
       """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

//  数据处理
    val resDF= readDF.select('id,
      when('gender === "1","男").
        when('gender === "2","女").
        otherwise("未知").
        as("gender"))
//    println(readDF.count())
//    resDF.show()

//    写入hbase
    val catalogwrite =
      """
        |{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"gender":{"cf":"cf","col":"gender","type":"string"}
        |}}
      """.stripMargin
    resDF.write
        .option(HBaseTableCatalog.tableCatalog, catalogwrite)
        .option(HBaseTableCatalog.newTable,"5")
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()



//    写入mysql
//    resDF.select('id.cast("int") as "id",'gender)
//      .write.format("jdbc").mode(SaveMode.Overwrite)
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_gender")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .save()

//    查看mysql数据
//    spark.read
//      .format("jdbc")
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_gender")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .load()
//      .show()


    spark.stop()

  }

}
