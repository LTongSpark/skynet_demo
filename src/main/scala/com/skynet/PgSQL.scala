package com.skynet

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.UUID

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.nutz.dao.Cnd

import scala.collection.JavaConverters._

object PgSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("app").master("local[*]").getOrCreate()

    val prop = new java.util.Properties()

    val tableName = "public.student"
    val url = "jdbc:postgresql://localhost/?zeroDateTimeBehavior=convertToNull"
    //添加数据库的username(user)密码(password),指定postgresql驱动(driver)
    prop.setProperty("user", "postgres")
    prop.setProperty("password", "root")
    prop.setProperty("driver", "org.postgresql.Driver")
    var jdbcDF = spark.read.jdbc(url, tableName, prop)

    val yyyyy = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val yyyyy1 = new SimpleDateFormat("yyyyMMddHHmmss")

    def genUUID = UUID.randomUUID().toString.replace("-","")
    spark.udf.register("genuuid" ,genUUID _)

    def parseTime(time:String) ={
      if(StringUtils.isNotBlank(time)){
        Timestamp.valueOf(yyyyy.format(yyyyy1.parse(time).getTime))
      }else{
        Timestamp.valueOf("1900-01-01 00:00:00")
      }

    }
    spark.udf.register("paseTime" ,parseTime _)
    import org.apache.spark.sql.functions._

    jdbcDF = jdbcDF.withColumn("ida" ,monotonically_increasing_id()).withColumn("changshu" ,lit("          123         45"))
      .withColumn("uuid" ,callUDF("genuuid")).withColumn("date" ,current_date()).withColumn("rksj" ,current_timestamp())
    jdbcDF.select(trim(jdbcDF.col("changshu")).as("changshu")).show(10 ,false)

    jdbcDF.select(when(jdbcDF("name")==="tong","poi").when(jdbcDF("name")==="liu","mnb").otherwise("cccccc")).show(10 ,false)
    jdbcDF.show(10 ,false)
    jdbcDF.withColumn("time_temp" ,callUDF("paseTime" ,jdbcDF.col("jssj"))).show(10 ,false)
    //jdbcDF.withColumn("time_temp" ,when(jdbcDF("jssj") =!=  null || jdbcDF("jssj") =!=  "" ,callUDF("paseTime" ,jdbcDF.col("jssj"))).otherwise("liu")).show(10 ,false)





    //    data.asScala.map(rdd =>{
//      val name = rdd.getString("name")
//      val res = JSON.parseArray(name)
//
//    })


  }

}
