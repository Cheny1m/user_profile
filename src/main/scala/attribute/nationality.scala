package attribute

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.when

//人口属性：国籍

object nationality {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("nationality")
      .master("local")
      .getOrCreate()

    import  spark.implicits._

//    写数据
    def catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_users"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"nationality":{"cf":"cf","col":"nationality","type":"string"}
        |}}
      """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
//    readDF.show()

//    数据处理
    val resultDF = readDF.select('id,
      when('nationality === "1","中国大陆")
        .when('nationality === "2","中国香港")
        .when('nationality === "3", "中国澳门")
        .when('nationality === "4", "中国台湾")
        .otherwise("其他")
        .as("nationality"))
//    resultDF.show()

    //    写数据
    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"nationality":{"cf":"cf","col":"nationality","type":"string"}
        |}}
      """.stripMargin
    resultDF.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()


//    查看运行结果，要先注释前面的写入操作
//    spark.read
//      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//      .show()


//    写入mysql
//    resultDF.select('id.cast("int") as "id",'nationality)
//      .write.format("jdbc").mode(SaveMode.Overwrite)
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_nationality")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .save()
//
//    查看mysql数据
//    spark.read
//      .format("jdbc")
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_nationality")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .load()
//      .show()



    spark.stop()

  }

}
