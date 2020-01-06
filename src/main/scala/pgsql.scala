import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Author: LTong
  * Date: 2019-11-22 下午 3:36
  */
object pgsql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ttt").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val prop = new java.util.Properties()

    val tableName = "qwe"
    val url = "jdbc:postgresql://localhost/?zeroDateTimeBehavior=convertToNull"
    var partion = new Array[String](2)
    partion(0) = s"1=1 limit 38 offset 0"
    partion(1) = s"1=1 limit 76 offset 38"
    //添加数据库的username(user)密码(password),指定postgresql驱动(driver)
    prop.setProperty("user", "postgres")
    prop.setProperty("password", "root")
    prop.setProperty("driver", "org.postgresql.Driver")



   var jdbcDF = spark.read.jdbc(url, s"(select * from  ${tableName}) t", partion, prop)
   // Thread.sleep(200000)
    println(jdbcDF.rdd.partitions.size)
    // jdbcDF = jdbcDF.repartition(19)
    //val count:Long = jdbcDF.first().get(0).toString.toLong

    jdbcDF.show(false)


//    println("====================================================" + count.toString)
//
//    println("======================================================" + jdbcDF.rdd.partitions.size)

//    val tuple = SchemaUtil.partition(count ,partion)
//    partion = new Array[String](tuple._1)
//    for(i <- 0 until partion.size){
//      print(i)
//      partion(i) = s"1=1 limit ${tuple._2} offset ${i * tuple._2}"
//    }
//    //Thread.sleep(200000)
//    //spark.sql(s"SELECT ${tableName}.*,(" + "@" +"rowNum:=@rowNum+1) AS rowNo FROM ${tableName}, (SELECT (" + "@" + "rowNum :=0) ) b ORDER BY rowNo DESC")
//
//    //jdbcDF = spark.read.jdbc(url ,s"(SELECT tong.* FROM tong  ORDER BY ctid ASC) t" ,prop).repartition(4).drop("ctid")
//
//    jdbcDF = spark.read.jdbc(url, s"(SELECT ctid ,tong.* FROM tong  ORDER BY ctid ASC) t", partion, prop).drop("ctid")
//    val at = jdbcDF.rdd.glom().collect()
//    for (elem <- at) {
//      println(elem.length)
//
//    }
//    //jdbcDF.show(false)
////    println("====================================================" + count)
////    println("======================================================" + jdbcDF.rdd.partitions.size)
//var list:ListBuffer[String] = new ListBuffer[String]
//
//    val tong = SchemaUtil.createDataFrame(spark, jdbcDF)
//    val data = tong.select("name")
//
//    val rows: Array[Row] = data.collect()
//    rows.foreach(rdd =>{
//      val arr = rdd.get(0).toString.split(",")
//      for(i <- 0 until arr.length){
//        list += arr(i)
//      }
//    })
//
////    data.foreachPartition(rdd =>{
////      rdd.foreach(rdd =>{
////        val arr = rdd.get(0).toString.split(",")
////        for(i <- 0 until arr.length){
////          list += arr(i)
////        }
////      })
////    })
//
//    print("=====================================" + list)



    //tong.write.format("csv").option("delimiter",",").option("header" ,"true").save(s"file:///d:/mr/tongpgsql")


  }

}
