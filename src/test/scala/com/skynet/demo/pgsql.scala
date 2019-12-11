package com.skynet.demo

import java.sql.{DriverManager, ResultSet}

object pgsql {
  def main(args: Array[String]): Unit = {

    val tong = "qwe12345678543"

    println(tong.replaceAll("3|4|q" ,"*"))
  }

}
