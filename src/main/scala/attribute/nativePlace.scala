package attribute


import org.apache.spark.sql.SparkSession
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import com.alibaba.fastjson.{JSON, JSONObject}
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
//    readDF.show()


//    数据处理
//    查询归属地所在市,调用阿里云api
    val getCity = (mobile: String)=> {
      val appcode = "244e9f8e5da04ce8b36568fa6d0fa92a"
      val httpClient = HttpClients.createDefault()    // 创建 client 实例
      val url = "http://jisusjhmcx.market.alicloudapi.com/shouji/query?shouji=" + mobile
      val get = new HttpGet(url)    // 创建 get 实例
      get.setHeader("Authorization","APPCODE " + appcode)
      get.setHeader("Content-Type", "application/json; charset=UTF-8")
      val response = httpClient.execute(get)    // 发送请求
      var jsonOBJ :JSONObject = JSON.parseObject(EntityUtils.toString(response.getEntity) )
//      println(mobile + jsonOBJ.getString("msg"))
      jsonOBJ  =  JSON.parseObject(jsonOBJ.getString("result") )
//      println(jsonOBJ)
      val res = jsonOBJ.getString("city")
      if(res == "") "未知" else res
    }
//    查询每个手机归属地
    val resultDF = readDF.rdd.map(row => {
      val s1 = row.getAs[String](0)
      val s2 = row.getAs[String](1)
      (s1,getCity(s2))
    }).toDF("id","nativePlace")
//    resultDF.show(950)

//    写数据
    def catalogwrite =
      """{
        |"table":{"namespace":"default","name":"user_profile"},
        |"rowkey":"id",
        |"columns":{
        |"id":{"cf":"rowkey","col":"id","type":"string"},
        |"nativePlace":{"cf":"cf","col":"nativePlace","type":"string"}
        |}}
      """.stripMargin
    resultDF.write
      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

//    查看运行结果，要先注释上面的数据处理和写数据操作
//    spark.read
//      .option(HBaseTableCatalog.tableCatalog, catalogwrite)
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .load()
//      .show(950)






    spark.stop()
  }
}
