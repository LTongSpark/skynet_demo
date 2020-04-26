package com.skynet

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object asd {

  case class tong(uuid1:String ,uuid:String ,name:String ,times:Int,list_qwe:String ,rksj:Timestamp ,min_time:Timestamp ,max_time:Timestamp ,input:String ,tong1:Double)
  def main(args: Array[String]): Unit = {


    for(i <- 27 to 43){
      println("" + i)
    }
  }

  def get(): Unit ={
    val  format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")  //234567.555555556000000000
    val spark = SparkSession.builder().appName("app").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    //    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    //    val b = a.keyBy(_.length)
    //    b.collect().foreach(println)
    //    b.groupByKey.collect


    val text = sc.parallelize(  Array(("A","2019-12-23 10:23:45","" ,24,35 ,""),("B","2019-12-26 10:23:45","liu" ,6,38,""),("12","2019-12-26 10:23:49","tong" ,6,38,"123"),("13","2019-12-01 10:23:45","tong" ,25,65,"123")))

    val value: RDD[(((String,String), (String ,String ,Int, Int ,String)), ((String,String), Int))] = text.map(rdd => (((rdd._3 ,rdd._6), (rdd._1,rdd._2  ,rdd._4  ,rdd._5,rdd.toString())), ((rdd._3,rdd._6), 1)))

    val data = value.map(_._2).reduceByKey(_+_)
    val data1 =  value.map(_._1)
    //data1.foreach(println)
    //println("===============================================================")
    /**
     * (tong,(2,(2019,24,35)))
     * (tong,(2,(2019,25,65)))
     * (liu,(1,(2016,23,78)))
     * val tong = (a._2._2._2 + a._2._2._3 + b._2._2._2 + b._2._2._3)/4
     * (a._1 ,tong)
     *
     *
     * (liu_1,CompactBuffer((23,78)))
     * (tong_2,CompactBuffer((24,35), (25,65)))
     * ( _1,CompactBuffer((25,65)))
     */

    val uuid = sc.applicationId

    //data.join(data1).sortBy(_._2._1, false).foreach(println)
    //println("===============================================================")
    //data.join(data1).sortBy(_._2._1, false).distinct().groupByKey().foreach(println)
    //println("===============================================================")
    val liu = data.join(data1).sortBy(_._2._1, false).distinct().groupByKey()

      .map(rdd =>{
        val uuid1 = UUID.randomUUID().toString.replace("-" ,"")
        var list_qwe:ListBuffer[String] = ListBuffer()
        val list_time:ListBuffer[String] = ListBuffer()
        var times:Int = 0
        var list_input:String = ""
        rdd._2.toList.foreach(rdd =>{
          times = rdd._1
          list_qwe += rdd._2._1
          list_time += rdd._2._2
          list_input += rdd._2._5
        })
        //234567.555555555560000000  234567.000000000000000000  234567.555555555560000000
        val tong1:Double = 234567.555555555555555555555555555555555555555555555
        val d = Timestamp.valueOf(format.format(new Date()))
        (uuid1 ,uuid ,rdd._1._1 ,rdd._2.size ,list_qwe.mkString(",") ,d,Timestamp.valueOf(list_time.min)
          ,Timestamp.valueOf(list_time.max) ,list_input ,tong1)
      })

        println("----------------------------------------")

        val prop = new java.util.Properties()

        val url = "jdbc:postgresql://localhost/?zeroDateTimeBehavior=convertToNull"
        //添加数据库的username(user)密码(password),指定postgresql驱动(driver)
        prop.setProperty("user", "postgres")
        prop.setProperty("password", "root")
        prop.setProperty("driver", "org.postgresql.Driver")

        import spark.implicits._
        liu.toDF().write.mode(SaveMode.Append).jdbc(url ,"qwe" ,prop)










    //      .reduce((a, b) => {
    //      val tong = (a._2._2._2 + a._2._2._3 + b._2._2._2 + b._2._2._3) / 4
    //      (a._1, (a._2._1, (a._2._2._1, tong, tong)))
    //    }))

    //      .groupByKey().map(rdd =>{
    //      (rdd._1 ,rdd._2.toList.sum /rdd._2.toList.size)
    //    }).collect().foreach(println)






  }

}
