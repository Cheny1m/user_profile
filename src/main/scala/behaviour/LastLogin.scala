package behaviour

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

/**
  * @author Tracy
  * @date 2021/6/23 10:05
  */

//行为特征：最近登录

object LastLogin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("lastLogin")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    //    读数据
    def catalog =
      """{
        |"table":{"namespace":"default","name":"tbl_users"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"lastLoginTime":{"cf":"cf","col":"lastLoginTime","type":"string"}
        |}}
      """.stripMargin

    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
//    readDF.show(20)

    var tempDF = readDF.select('id.cast("int") as "id",'lastLoginTime.cast("int") as "lastLoginTime")
      .sort('lastLoginTime.desc)
    val lasttime = tempDF.head()(1)
    tempDF = tempDF.select('id,abs('lastLoginTime - lasttime) as "interval")
//    tempDF.show()

    val result = tempDF.select('id,
      when('interval > 14*24*60*60 ,"30天内")
        .when('interval > 7*24*60*60 ,"14天内")
        .when('interval > 24*60*60 ,"7天内")
        .otherwise("1天内")
        .as("lastLogin"))
//    result.show()

//    写入hbase
    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"lastLogin":{"cf":"cf","col":"lastLogin","type":"string"}
        |}}
      """.stripMargin
    result
      .where('id <= 950)
      .select('id.cast("string") as "id",'lastLogin)
      .write
      .option(HBaseTableCatalog.tableCatalog,catalogwrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

//    查看结果，需注释上面写操作再查看
//    spark.read
//      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//      .show()

//    写入mysql
//    result
//      .write.format("jdbc").mode(SaveMode.Overwrite)
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_lastLogin")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .save()

//    查看mysql数据
//    spark.read
//      .format("jdbc")
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_lastLogin")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .load()
//      .show(950)


  }
}
