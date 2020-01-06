package com.skynet

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.nutz.Nutz
import org.nutz.dao.Cnd
import org.nutz.dao.impl.{NutDao, SimpleDataSource}

import scala.collection.JavaConverters._
object json {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("app").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val url = "jdbc:mysql://localhost:8888/ycdata?zeroDateTimeBehavior=convertToNull&autoReconnect=true&failOverReadOnly=false&useSSL=false"
    val tableName = "student"
    // 设置连接用户&密码
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    prop.setProperty("driver" ,"com.mysql.jdbc.Driver")

    val data = spark.read.jdbc(url ,"student" ,prop).select("name1")
    val value = data.rdd.map(rdd =>(rdd.getAs[String]("name1")))

    val qwe1 = value.map(JSON.parseArray).flatMap(_.toArray).map(_.asInstanceOf[JSONObject])
      .map(x =>(x.getString("flag"),x.getString("time") ,x.getString("lat") ,x.getString("lin")))

    println("===========================================================================")

    val dao = new NutDao()
    val ds = new SimpleDataSource()
    ds.setDriverClassName("com.mysql.jdbc.Driver")
    ds.setJdbcUrl("jdbc:mysql://localhost:8888/ycdata?zeroDateTimeBehavior=convertToNull&autoReconnect=true&failOverReadOnly=false&useSSL=false")
    ds.setUsername("root")
    ds.setPassword("root")

    dao.setDataSource(ds)

    val data1 = dao.query("student" ,null)
    val qwe = data1.asScala.map(rdd =>
      (rdd.getString("name1"))
    ).map(JSON.parseArray).flatMap(_.toArray).map(_.asInstanceOf[JSONObject]).map(x =>(x.getString("flag"),x.getString("time") ,x.getDouble("lat") ,x.getDouble("lin")))
    val sc1 = sc.parallelize(qwe)

    val wqe2 = data1.asScala.map(rdd =>(rdd.getInt("age") ,rdd.getInt("age").toString))
    val sc2 = sc.parallelize(wqe2)
    sc1.cartesian(sc2).map(rdd =>(rdd._1.asInstanceOf[Tuple4[scala.Predef.String ,scala.Predef.String ,Double ,Double]] ,rdd._2._1 ,rdd._2._2)).map(rdd =>{
      tong(rdd ,spark)
    }).foreach(println)
  }

  def tong(point:Tuple3[Tuple4[String ,String ,Double ,Double] ,Int ,String] ,sparkSession: SparkSession): String ={
    println(point)
    "qqqqqqqqqqqqqq"
  }

}
