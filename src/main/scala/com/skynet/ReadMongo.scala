package com.skynet

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession

object ReadMongo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MyApp")
      .getOrCreate()


    var map: Map[String, String] = Map()
    map += ("spark.mongodb.input.uri" -> "mongodb://127.0.0.1/runoob")
    map += ("spark.mongodb.input.partitioner.MongoPaginateBySizePartitioner" -> "10")

    val list = List("runoob", "col")
    for (elem <- list) {

      // 设置log级别
      spark.sparkContext.setLogLevel("WARN")
      map += ("spark.mongodb.input.collection" -> elem)

      val df = MongoSpark.load(spark, ReadConfig(map)).orderBy("_id")
          println("=========================" + df.rdd.partitioner.size)

      df.show(false)
    }


    spark.stop()
    System.exit(0)
  }
}
