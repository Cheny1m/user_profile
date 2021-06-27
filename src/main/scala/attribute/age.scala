package attribute

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

//人口属性：年龄段

object age {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("age")
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
        |"birthday":{"cf":"cf","col":"birthday","type":"string"}
        |}}
      """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
//    readDF.show()

//    数据处理
    val resDF = readDF.select('id,year(to_timestamp('birthday,"yyyy-MM-dd")) as "year")
      .select('id,when('year>=2020,"20后")
        .when('year>=2010,"10后")
        .when('year>=2000,"00后")
        .when('year>=1990,"90后")
        .when('year>=1980,"80后")
        .when('year>=1970,"70后")
        .when('year>=1960,"60后")
        .when('year>=1950,"50后")
        .otherwise("其他")
        .as("age"))
//    resDF.show()

//  写数据，写入hbase
    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"age":{"cf":"cf","col":"age","type":"string"}
        |}}
      """.stripMargin
    resDF.write
      .option(HBaseTableCatalog.tableCatalog,catalogwrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

//    查看habse数据，需注释上面写操作再查看
//    spark.read
//      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//      .show()


//    写入mysql
//    resDF.select('id.cast("int") as "id",'age)
//      .write.format("jdbc").mode(SaveMode.Overwrite)
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_age")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .save()

//    查看mysql数据
//    spark.read
//      .format("jdbc")
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_age")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .load()
//      .show()


    spark.stop()


  }

}
