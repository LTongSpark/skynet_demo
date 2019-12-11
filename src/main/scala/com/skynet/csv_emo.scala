package com.skynet

import org.apache.spark.sql.SparkSession

object csv_emo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MysqlQueryDemo").master("local[*]")
      .config("spark.driver.maxResultSize", "10g").getOrCreate()
    val data = spark.read.format("csv").option("delimiter",",").option("encoding","gbk").option("multiLine", "true").option("header" ,"true").load(s"file:///d:/mr/tong.csv")
    data.show(false)
  }

}
