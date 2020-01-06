import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Author: LTong
  * Date: 2019-10-24 下午 5:52
  */
object MysqlDemo {
  def main(args: Array[String]): Unit = {

    //print(schema)
    val spark = SparkSession.builder().appName("MysqlQueryDemo").master("local[*]").config("spark.driver.maxResultSize", "10g").getOrCreate()

    val url = "jdbc:mysql://localhost:8888/ycdata?zeroDateTimeBehavior=convertToNull&autoReconnect=true&failOverReadOnly=false&useSSL=false"
    val tableName = "biz_log_isddl1"
    var partion = new Array[String](1)
    // 设置连接用户&密码
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    var jdbcDF = spark.read.jdbc(url, s"(select count(1) from  ${tableName}) t", partion, prop)

    val count: Long = jdbcDF.first().get(0).toString.toLong

    val tuple = SchemaUtil.partition(count, partion)
//    partion = new Array[String](tuple._1)
//        for (i <- 0 until partion.size) {
//          partion(i) = s"1=1 limit ${i * tuple._2},${tuple._2}"
//        }

    //    jdbcDF = spark.read.jdbc(url ,s"(SELECT (@rowNum:=@rowNum+1) AS rowNo ,${tableName}.* FROM ${tableName}, (SELECT (@rowNum :=0)) b ORDER BY rowNo ASC) t" ,prop)
    //        .repartition(20)
    jdbcDF = spark.read.jdbc(url, s"(SELECT (@rowNum:=@rowNum+1) AS rowNo ,${tableName}.* FROM ${tableName}, (SELECT (@rowNum :=0)) b) t", "rowNo",
      0, count, partion.size, prop)

    println(jdbcDF.count())

    var tong = SchemaUtil.createDataFrame(spark, jdbcDF)
    //tong.show(100,false)
    import org.apache.spark.sql.functions.concat_ws

    val data = tong.select(tong.col("user_type"),
      tong.col("user_id"), tong.col("infotype"))



    data.rdd.foreach(println)
    println("==============================================")
    data.dropDuplicates(Array("user_type")).rdd.foreach(println)

    println("==============================================")
    data
      .rdd.map(rdd => {
      val tong = rdd.toString().substring(1, rdd.toString().length - 1).split(",")
      if (tong.length > 1) (tong(0), tong(1))
      else Nil
    }).filter(!_.equals(Nil)).map(_.asInstanceOf[Tuple2[String ,String]]).foreach(println)
    // tong.show(false)

    //    tong.write.format("csv").option("multiLine", true).save(s"file:///d:/mr/tong5")
    //    val data = spark.read.format("csv").option("multiLine", true).load(s"file:///d:/mr/tong5")
    //    println(data.count())

    //tong.show(100,false)
  }

  def saveCsv(data: DataFrame, path: String): Unit = {
    data.write.format("csv").option("header", "true").save(path)
  }

  case class User(id: Int, studentid: Int, name: String, age: Int, sex: String, birthday: String)

}
