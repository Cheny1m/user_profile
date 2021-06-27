package net.suncaper.tags.model

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
//客服咨询频率,已测试
object CustomerServiceFrequency {
  def main(args: Array[String]): Unit = {
    def catalog =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_orders"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"},
         |    "memberId":{"cf":"cf", "col":"memberId", "type":"string"},
         |    "smConfirmStatus":{"cf":"cf", "col":"smConfirmStatus", "type":"string"}
         |  }
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    /*val DF = spark.read.format("jdbc")
       .option("url", "jdbc:mysql://localhost:3306/tags_dat")
       .option("dbtable", "tbl_orders")
       .option("user", "root")
       .option("password", "123456")
       .option("partitionColumn", "id")
       .option("lowerBound", 1)
       .option("upperBound", 60)
       .option("numPartitions", 10)
       .load()
      .na.drop(List("smConfirmStatus"))

    val temp = DF
      .where('smConfirmStatus===3 or ('smConfirmStatus ===4))
      .groupBy('memberId)
      .agg(count('smConfirmStatus) as "count")

    val temp2 = DF
      .where('smConfirmStatus===3)
      .groupBy('memberId)
      .agg(count('smConfirmStatus) as "PersonCount")

    val result = temp.join(temp2,temp.col("memberId")===temp2.col("memberId"))
      .withColumn("CustomerServiceFrequency",col("PersonCount")/col("count"))
      .select(temp.col("memberId"),
        when('CustomerServiceFrequency>0.15,"高")
          .when('CustomerServiceFrequency<=0.15
            and('CustomerServiceFrequency>0.09),"中")
          .otherwise("低")
          .as("CustomerServiceFrequency")
      )
      .show()*/


   val source: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
      .na.drop(List("smConfirmStatus"))

      val temp = source
      .where('smConfirmStatus===3 or ('smConfirmStatus ===4))
      .groupBy('memberId)
      .agg(count('smConfirmStatus) as "count")

    val temp2 = source
      .where('smConfirmStatus===3)
      .groupBy('memberId)
      .agg(count('smConfirmStatus) as "PersonCount")

    val result = temp.join(temp2,temp.col("memberId")===temp2.col("memberId"))
      .withColumn("CustomerServiceFrequency",col("PersonCount")/col("count"))
      .select(temp.col("memberId") as "idd" ,
        when('CustomerServiceFrequency>0.15,"高")
          .when('CustomerServiceFrequency<=0.15
            and('CustomerServiceFrequency>0.09),"中")
          .otherwise("低")
          .as("CustomerServiceFrequency")
      )


    def catalog_user =
      s"""{
         |  "table":{"namespace":"default", "name":"tbl_users"},
         |  "rowkey":"id",
         |  "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"long"}
         |  }
         |}""".stripMargin

    val source_user: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog_user)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val res = source_user.join(result,source_user.col("id")===result.col("idd"),"left")
      .select('id,'CustomerServiceFrequency)

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"user_profile"},
         |"rowkey":"id",
         |"columns":{
         |  "id":{"cf":"rowkey", "col":"id", "type":"string"},
         |  "CustomerServiceFrequency":{"cf":"cf", "col":"CustomerServiceFrequency", "type":"string"}
         |}
         |}""".stripMargin

    res.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
