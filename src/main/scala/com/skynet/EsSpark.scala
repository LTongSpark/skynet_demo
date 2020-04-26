package com.skynet

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.sql.EsSparkSQL
import org.elasticsearch.spark.streaming.EsSparkStreaming

object EsSpark {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("app").master("local[*]")//.config("es.nodes", "s112")
      .getOrCreate()
//      .config("es.index.auto.create", "true")
//      .config("es.port", "9200")
//      .getOrCreate()

//    val df: DataFrame = spark.sql("select * from tong")
//
//    EsSpark
//    EsSparkSQL.saveToEs(df, "", Map("" -> ""))
//    df.saveToEs("" ,Map("" ->""))

    val sc = spark.sparkContext
    val rdd = sc.parallelize(List((1, List(1,2,2)),(1, List(1,"tong",2)), (3, List(3,3,3))))
    val rdd08_1 = rdd.reduceByKey((x, y) => x.++(y))
    rdd08_1.foreach(println)

    val date = new Date()
    val sim = new SimpleDateFormat("yyyy-MM-dd")
    println(sim.format(date))

  }

}
