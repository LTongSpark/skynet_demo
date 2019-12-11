package com.skynet.demo

import java.util.UUID

import org.json4s.native.Json
import org.json4s.DefaultFormats



object test {
  def main(args: Array[String]): Unit = {

    println(UUID.randomUUID().toString.replaceAll("-" ,""))
  }
}
