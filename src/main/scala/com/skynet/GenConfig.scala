package com.skynet

import java.util.Properties

object GenConfig {
  private val prop = new Properties
  prop.load(GenConfig.getClass.getClassLoader.getResourceAsStream("gen.properties"))

  def getString(key: String): String = {
    prop.getProperty(key)
  }
  def getInt(key: String): Int = {
    prop.getProperty(key).toInt
  }
  def getBoolean(key: String): Boolean = {
    prop.getProperty(key).toBoolean
  }
  def getLong(key: String): Long = {
    prop.getProperty(key).toLong
  }


  def main(args: Array[String]): Unit = {
    println(getString("author"))
  }

}
