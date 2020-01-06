package com.skynet

import org.apache.spark.sql.{DataFrame, SparkSession}

object csv_emo {
  def main(args: Array[String]): Unit = {
    val aa: Double = getDistatce(40.00694, 116.394476, 40.007538, 116.375561)
    val map = squarePoint(40.00694, 116.394476, 1000)
    val map1 = squarePoint(40.00694, 116.394476, aa)
    println(map)
    println(map1)
    println(aa)
  }

  //根据经纬度计算两点距离
  def getDistatce(lat1: Double, lon1: Double, lat2: Double, lon2: Double) = {
    if (lat1 != 0 && lon1 != 0 && lat2 != 0 && lon2 != 0) {
      val R = 6378.137
      val radLat1 = lat1 * Math.PI / 180
      val red = Math.toRadians(lat1)
      val radLat2 = lat2 * Math.PI / 180
      val a = radLat1 - radLat2
      val b = lon1 * Math.PI / 180 - lon2 * Math.PI / 180
      val s = 2 * Math.sin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
      BigDecimal.decimal(s * R).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    } else {
      BigDecimal.decimal(0).setScale(2, BigDecimal.RoundingMode.HALF_UP).doubleValue()
    }
  }

  def squarePoint(lat1: Double, long1: Double, distance: Double): Map[String, (Double, Double)] = {

    val dLong = Math.toDegrees(2 * (Math.asin(Math.sin(distance / (2 * 6378.137)) / Math.cos(Math.toRadians(lat1)))))

    //计算维度坐标
    val dlat = Math.toDegrees(distance / 6378.137)

    Map(
      "left_top" -> (lat1 + dlat, long1 -dLong),
      "right_top" -> (lat1 + dlat, long1 + dLong),
      "left_bottom" -> (lat1 - dlat, long1 - dLong),
      "right_bottom" -> (lat1 - dlat, long1 + dLong)
    )
  }


}
