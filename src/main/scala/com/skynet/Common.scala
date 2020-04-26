package com.skynet

import java.util

import org.nutz.dao.{Chain, Sqls}
import org.nutz.dao.entity.Record
import org.nutz.dao.impl.{NutDao, SimpleDataSource}

object Common {
  def GetDao():NutDao ={
    val dao = new NutDao()
    val ds = new SimpleDataSource()
    ds.setDriverClassName("org.postgresql.Driver")
    ds.setJdbcUrl("jdbc:postgresql://localhost/?zeroDateTimeBehavior=convertToNull")
    ds.setUsername("postgres")
    ds.setPassword("root")
    dao.setDataSource(ds)
    dao
  }

  def main(args: Array[String]): Unit = {
    val dao = GetDao()
    dao.update()
    val json = new util.ArrayList[Object]()
    val record =  new Record()
    val array = Array("tong",2,3,4,5,6,7)
    for (elem <- 0 until array.length) {
      json.add(record.put("id" ,array(elem)))
      if(json.size() > 3){
        json
      }
    }

    val sql = Sqls.create("")

    sql.addBatch()

    println(json)

  }


}
