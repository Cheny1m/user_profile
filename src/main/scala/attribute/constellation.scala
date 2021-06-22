package attribute

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._


//人口属性：星座
object constellation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("constellation")
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
        |"birthday":{"cf":"cf","col":"birthday","type":"string"}
        |}}
      """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
//    readDF.show()

//    数据处理
    val resultDF = readDF.select('id,month(to_timestamp('birthday,"yyyy-MM-dd")) as "month",dayofmonth(to_timestamp('birthday,"yyyy-MM-dd")) as "day")
        .select('id,
          when(('month === 3 and 'day >= 21) or ('month === 4 and 'day <= 19),"白羊座" )
            .when(('month === 4 and 'day >= 20) or ('month === 5 and 'day <= 20),"金牛座")
            .when(('month === 5 and 'day >= 21) or ('month === 6 and 'day <= 21),"双子座")
            .when(('month === 6 and 'day >= 22) or ('month === 7 and 'day <= 22),"巨蟹座")
            .when(('month === 7 and 'day >= 23) or ('month === 8 and 'day <= 22),"狮子座")
            .when(('month === 8 and 'day >= 23) or ('month === 9 and 'day <= 22),"处女座")
            .when(('month === 9 and 'day >= 23) or ('month === 10 and 'day <= 23),"天秤座")
            .when(('month === 10 and 'day >= 24) or ('month === 11 and 'day <= 22),"天蝎座")
            .when(('month === 11 and 'day >= 23) or ('month === 12 and 'day <= 21),"射手座")
            .when(('month === 12 and 'day >= 22) or ('month === 1 and 'day <= 19),"摩羯座")
            .when(('month === 1 and 'day >= 20) or ('month === 2 and 'day <= 18),"水瓶座")
            .otherwise("双鱼座")
            .as("constellation"))
//    resultDF.show()

//    写数据
    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"constellation":{"cf":"cf","col":"constellation","type":"string"}
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

    spark.stop()
  }

}
