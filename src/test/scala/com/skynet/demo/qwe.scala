package com.skynet.demo

import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import org.json4s.native.Json
import org.json4s.DefaultFormats
import org.apache.spark.sql.SparkSession
object qwe {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MysqlQueryDemo").master("local[*]").config("spark.driver.maxResultSize", "10g").getOrCreate()



    val url = "jdbc:mysql://localhost:8888/ycdata?zeroDateTimeBehavior=convertToNull&autoReconnect=true&failOverReadOnly=false"
    // 设置连接用户&密码
    val prop = new java.util.Properties
    var partion = new Array[String](1)
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    var count1:Long = 0

    val list = List("student","student1" ,"student2")
    val start_time1 = getCurrent()
    var map :Map[String ,Any] = Map()
    var ex = ""
    var sjy_id = 0
    var sjtcrw_id = 0
    var table = ""
    var stat = 0
    var cwxx  =""
    var id = UUID.randomUUID().toString.replace("-" ,"")
    insertTask(id ,sjy_id ,sjtcrw_id ,table ,stat ,start_time1 ,start_time1 ,cwxx)
    for(table <- list){
      var start_time:Timestamp = null
      var end_time:Timestamp = null
      var count:Long = 0l
      var count_res:Long = 0l
      var id1 = UUID.randomUUID().toString.replace("-" ,"")
      try {
        start_time = getCurrent()
        var jdbcDF = spark.read.jdbc(url, s"(select count(1) from  ${table}) t", partion, prop)
        count= jdbcDF.first().get(0).toString.toLong

        jdbcDF = spark.read.jdbc(url, s"(select * from $table) t", prop)

        jdbcDF.write.format("csv").option("multiLine", true).save(s"file:///d:/mr/tong5_$table")
        end_time = getCurrent()
        val data = spark.read.format("csv").option("multiLine", true).load(s"file:///d:/mr/tong5_$table")
        count_res = data.count()
        insertData(id1 ,sjy_id ,sjtcrw_id ,table ,start_time ,end_time ,count ,count_res)
      } catch {
        case e :UnsupportedOperationException =>{
          insertData(id1 ,sjy_id ,sjtcrw_id ,table ,start_time ,end_time ,count ,count_res)
        }
        case exception: Exception =>{
          val end_time1 = getCurrent()
          cwxx = exception.fillInStackTrace().toString
          stat = 2
          insertTask(id ,sjy_id ,sjtcrw_id ,table ,stat ,start_time1 ,end_time1 ,cwxx)
        }
      }
    }
    if(cwxx.length == 0){
      val end_time1 = getCurrent()
      stat = 1
      insertTask(id ,sjy_id ,sjtcrw_id ,table ,stat ,start_time1 ,end_time1 ,cwxx)
    }

  }

  def insertData(id:String ,sjy_id:Int ,sjtcrw_id:Int ,table:String ,start_time: Timestamp ,end_time:Timestamp ,sjxr:Long ,read:Long): Unit ={
    getConn(s"insert into tong(id,sjy_id ,sjtcrw_id ,bm,kssj,jssj,sjxr,cqsl) values('$id','$sjy_id','$sjtcrw_id','$table' ,'$start_time' ,'$end_time' ,'$sjxr' ,'$read')")
  }

  def insertTask(id:String ,sjy_id:Int ,sjtcrw_id:Int ,table:String,stat:Int ,start_time: Timestamp ,end_time:Timestamp ,cwxx:String): Unit ={
    getConn(s"UPDATE tong set rwzt=$stat ,jssj='$end_time',cwxx='$cwxx' WHERE id='$id'")
    if(stat ==0){
      println("111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
      getConn(s"insert into tong(id,sjy_id ,sjtcrw_id ,bm,kssj,jssj, rwzt,cwxx) " +
        s"values('$id','$sjy_id','$sjtcrw_id','$table' ,'$start_time' ,'$end_time' ,'$stat','$cwxx')")
    }
  }

  def getConn(sql:String): Unit ={
    val conn = DriverManager.getConnection("jdbc:postgresql://localhost/?zeroDateTimeBehavior=convertToNull","postgres" ,"root")
    conn.setAutoCommit(false)
    val stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY ,ResultSet.CONCUR_READ_ONLY)
    //stat.addBatch(sql)
    stat.execute(sql)
    conn.commit()
    stat.close()
    conn.close()
  }

  def getCurrent(): Timestamp ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val str = df.format(new Date())
    Timestamp.valueOf(str)
  }

}
