package com.skynet

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.nutz.dao.entity.Record

import scala.collection.mutable.ListBuffer

object tong {
  def main(args: Array[String]): Unit = {
    val tome_yy = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    val tong = "19900101000000"
    val time1:Timestamp = null

    val time = new Timestamp(LocalDateTime.parse(tong, tome_yy).toInstant(ZoneOffset.of("+8")).toEpochMilli)

    val dao = Common.GetDao()
    val record = new Record()
    record.put("id" ,time)
    dao.insert("time" ,record.toChain)



  }
}
