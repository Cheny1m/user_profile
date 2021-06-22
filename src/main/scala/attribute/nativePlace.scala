package attribute


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog




object nativePlace {
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
        |"mobile":{"cf":"cf","col":"mobile","type":"string"}
        |}}
      """.stripMargin
    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    readDF.show()


//    数据处理
//    查询归属地
    def getCity(mobile: String): String = {
      val appcode = "244e9f8e5da04ce8b36568fa6d0fa92a"
      val httpClient = HttpClients.createDefault()    // 创建 client 实例
      val url = "http://jshmgsdmfb.market.alicloudapi.com/shouji/query?shouji=" + mobile
      val get = new HttpGet(url)    // 创建 get 实例
      get.setHeader("Authorization","APPCODE " + appcode)
      val response = httpClient.execute(get)    // 发送请求
      var jsonOBJ :JSONObject  =  JSON.parseObject(JSON.parseObject(EntityUtils.toString(response.getEntity) ).getString("result") )
      jsonOBJ.getString("city")
    }
//    查询每个手机归属地
//    readDF.rdd.map(x=>(x(0),getCity(x(1).asInstanceOf[String]))).toDF().show()




    spark.stop()
  }
}
