package com.skynet

import org.apache.spark.sql.SparkSession

object ExcelDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("app").master("local[*]").getOrCreate()

    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", "Info")
      .option("useHeader", true)
      .option("inferSchema" ,false)
      //.option("encoding" ,"utf-8")
      .load("file:///D:/mr/people.xlsx")

//    val  data = df.repartition(1).write.format("csv").option("encoding" ,"UTF-8").option("header" ,true).save("file:///D:/mr/people")
//
//    val data1 = spark.read
//      .format("csv")
//      .option("header", true)
//      .option("inferSchema" ,false)
//      .option("encoding" ,"UTF-8")
//      .load("file:///D:/mr/people")



    df.printSchema()

    df.show(false)
  }
}
