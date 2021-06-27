package attribute

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._

//人口属性：职业

object job {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("job")
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
        |"job":{"cf":"cf","col":"job","type":"string"}
        |}}
      """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
//    readDF.show()

//    数据处理
    val resultDF = readDF.select('id,
      when('job === "1","学生")
        .when('job === "2","公务员")
        .when('job === "3", "军人")
        .when('job === "4","警察")
        .when('job === "5","教师")
        .when('job === "6","白领")
        .otherwise("未知")
        .as("job"))
//    resultDF.show()

//    写入hbase
    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"job":{"cf":"cf","col":"job","type":"string"}
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
//    resultDF.select('id.cast("int") as "id",'job)
//      .write.format("jdbc").mode(SaveMode.Overwrite)
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_job")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .save()
//
//    查看mysql数据
//    spark.read
//      .format("jdbc")
//      .option("url","jdbc:mysql://master:3306/tags_dat?useUnicode=true&characterEncoding=utf8")
//      .option("dbtable","up_job")
//      .option("user","root")
//      .option("password","mysqlroot")
//      .load()
//      .show()




    spark.stop()

  }

}
