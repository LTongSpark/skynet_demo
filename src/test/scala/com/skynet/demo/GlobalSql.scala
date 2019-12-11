package com.skynet.demo

import java.sql.{DriverManager, ResultSet}

object GlobalSql {
  def main(args: Array[String]): Unit = {

    Class.forName("org.postgresql.Driver").newInstance
    val conn = DriverManager.getConnection("jdbc:postgresql://localhost/?zeroDateTimeBehavior=convertToNull",
    "postgres",
    "root")
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // Execute Query
      val rs = statement.executeQuery("SELECT * FROM student LIMIT 5")
      // Iterate Over ResultSet
      while (rs.next) {
        val fild = rs.getString("id") + "-" + rs.getString("name") + "-" + rs.getString("age")
        val split: Array[String] = fild.split("-")
        if(split(2).equals("null")){
          println(split(2))
        }
      }
    }
    finally {
      conn.close
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

}
