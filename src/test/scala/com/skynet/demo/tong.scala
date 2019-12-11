package com.skynet.demo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

object tong {


  def main(args: Array[String]): Unit = {
   println(getCurrent())
  }

  def getCurrent(): Timestamp ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val str = df.format(new Date())
    Timestamp.valueOf(str)
  }

}
